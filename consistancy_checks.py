import json
import gzip
import random
from pathlib import Path

def map_statement_type(statement_type):
    """Map statement type to shorter version"""
    mapping = {"ownershipOrControlStatement": 'ownership', "personStatement": 'person', "entityStatement": 'entity'}
    return mapping[statement_type]


class ConsistencyChecks:
    """Perform consistancy check on BODS data"""
    def __init__(self, source_dir, dates=False, hours=False, gzip=True, check_is_component=True):
        """Initialise"""
        print("Initialising consistency checks on data")
        self.statements = {}
        self.references = set()
        self.stats = {}
        self.source_dir = Path(source_dir)
        self.dates = dates
        self.hours = hours
        self.gzip = gzip
        self.check_is_component = check_is_component

    def statement_stats(self, statement):
        """Create stats data for BODs statement"""
        if statement['statementID'] in self.statements:
            self.statements[statement['statementID']]['count'] += 1
        else:
            self.statements[statement['statementID']] = {'count': 1, 'type': map_statement_type(statement['statementType'])}
        if statement['statementType'] == "ownershipOrControlStatement":
            self.references.add(statement['subject']["describedByEntityStatement"])
            if "describedByPersonStatement" in statement["interestedParty"]:
                self.references.add(statement["interestedParty"]["describedByPersonStatement"])
            elif "describedByEntityStatement" in statement["interestedParty"]:
                self.references.add(statement["interestedParty"]["describedByEntityStatement"])

    def check_statement(self, statement):
        """Check BODS statement fields"""
        assert 'statementID' in statement, f"No statementID in statement: {statement}"
        assert 'statementType' in statement, f"No statementType in statement: {statement}"
        assert 'publicationDetails' in statement, f"No publicationDetails in statement: {statement}"
        assert 'publicationDate' in statement['publicationDetails'], f"No publicationDetails/publicationDate in statement: {statement}"
        assert 'bodsVersion' in statement['publicationDetails'], f"No publicationDetails/bodsVersion in statement: {statement}"
        if self.check_is_component: assert 'isComponent' in statement, f"No isComponent in statement: {statement}"
        if statement['statementType'] == "personStatement":
            assert 'personType' in statement, f"No personType in person statement: {statement}"
            if statement['personType'] in ('anonymousPerson', 'unknownPerson'):
                assert 'reason' in statement['unspecifiedPersonDetails'], \
                        f"No reason for person statement with {statement['personType']} personType: {statement}"
        elif statement['statementType'] == "entityStatement":
            assert 'entityType' in statement, f"No entityType in entity statement: {statement}"
            if statement['entityType'] in ('anonymousEntity' or 'unknownEntity'):
                assert 'reason' in statement['unspecifiedEntityDetails'], \
                        f"No reason for entity statement with {statement['entityType']} entityType: {statement}"
        elif statement['statementType'] == "ownershipOrControlStatement":
            assert 'subject' in statement, f"No subject in ownershipOrControlStatement: {statement}"
            assert 'describedByEntityStatement' in statement['subject'], \
                    f"No subject/describedByEntityStatement in ownershipOrControlStatement: {statement}"
            assert 'interestedParty' in statement, f"No interestedParty in ownershipOrControlStatement: {statement}"
        else:
            assert False, f"Incorrect statementType for statement: {statement}"

    def read_json_file(self, f):
        """Read from JSON Lines file and yield items"""
        if self.gzip:
            with gzip.open(f, "r") as json_file:
                for line in json_file.readlines():
                    yield json.loads(line)
        else:
            with open(f, "r") as json_file:
                for line in json_file.readlines():
                    yield json.loads(line)

    def process_file(self, f):
        """Process input file"""
        for statement in self.read_json_file(f):
            self.check_statement(statement)
            self.statement_stats(statement)

    def read_data(self):
        """Read data from source directory"""
        print("Reading data from source directory")
        if self.dates:
            for month in Path(self.source_dir).iterdir():
                for day in month.iterdir():
                    if self.hours:
                        for hour in day.iterdir():
                            for f in hour.iterdir():
                                self.process_file(f)
                    else:
                        for f in day.iterdir():
                            self.process_file(f)
        else:
             for f in Path(self.source_dir).iterdir():
                 self.process_file(f)

    def generate_stats(self):
        """Generate stats for statements"""
        print("Generating statistics from BODS statements")
        for statement in self.statements:
            if not self.statements[statement]['count'] in self.stats:
                self.stats[self.statements[statement]['count']] = {'count': 0, 'ownership': set(), 'entity': set(), 'person': set()}
            self.stats[self.statements[statement]['count']]['count'] += 1
            self.stats[self.statements[statement]['count']][self.statements[statement]['type']].add(statement)

    def check_references(self):
        """Check internal references within BODS data"""
        print("Checking internal references with BODS data")
        for reference in self.references:
            assert reference in self.stats[1]['entity'] or reference in self.stats[1]['person'], f"Statement {reference} not found in input data"

    def check_stats(self):
        """Check statistics for data"""
        assert len(self.stats) == 1 and next(iter(self.stats)) == 1, "Duplicate statementIDs in input data"
        self.check_references()
        print(self.references)

    def run(self):
        """Run consistency checks"""
        self.read_data()
        self.generate_stats()
        self.check_stats()
