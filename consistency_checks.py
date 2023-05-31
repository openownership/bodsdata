import json
import gzip
import random
from pathlib import Path
from rich.console import Console

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
        self.error_log = []
        self.console = Console()

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

    def perform_check(self, check, message, extra_errors=False):
        """Perform check and log if there is an error"""
        if not check:
            if extra_errors:
                extra_errors(message)
            else:
                self.error_log.append(message)

    def check_statement(self, statement):
        """Check BODS statement fields"""
        self.perform_check('statementID' in statement, f"Missing BODS field: No statementID in statement: {statement}")
        self.perform_check('statementType' in statement, f"Missing BODS field: No statementType in statement: {statement}")
        self.perform_check('publicationDetails' in statement, f"Missing BODS field: No publicationDetails in statement: {statement}")
        self.perform_check('publicationDate' in statement['publicationDetails'], f"Missing BODS field: No publicationDetails/publicationDate in statement: {statement}")
        self.perform_check('bodsVersion' in statement['publicationDetails'], f"Missing BODS field: No publicationDetails/bodsVersion in statement: {statement}")
        if self.check_is_component:
            self.perform_check('isComponent' in statement, f"Missing BODS field: No isComponent in statement: {statement}")
        if statement['statementType'] == "personStatement":
            self.perform_check('personType' in statement, f"Missing BODS field: No personType in person statement: {statement}")
            if statement['personType'] in ('anonymousPerson', 'unknownPerson'):
                self.perform_check('reason' in statement['unspecifiedPersonDetails'], \
                        f"Missing BODS field: No reason for person statement with {statement['personType']} personType: {statement}")
        elif statement['statementType'] == "entityStatement":
            self.perform_check('entityType' in statement, f"Missing BODS field: No entityType in entity statement: {statement}")
            if statement['entityType'] in ('anonymousEntity' or 'unknownEntity'):
                self.perform_check('reason' in statement['unspecifiedEntityDetails'], \
                        f"Missing BODS field: No reason for entity statement with {statement['entityType']} entityType: {statement}")
        elif statement['statementType'] == "ownershipOrControlStatement":
            self.perform_check('subject' in statement, f"Missing BODS field: No subject in ownershipOrControlStatement: {statement}")
            self.perform_check('describedByEntityStatement' in statement['subject'], \
                    f"Missing BODS field: No subject/describedByEntityStatement in ownershipOrControlStatement: {statement}")
            self.perform_check('interestedParty' in statement, f"Missing BODS field: No interestedParty in ownershipOrControlStatement: {statement}")
        else:
            self.perform_check(False, f"BODS field value: Incorrect statementType for statement: {statement}")

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

    def check_reference(self, reference):
        """Check internal reference exists"""
        found = False
        for s in self.stats:
            if reference in self.stats[s]['entity'] or reference in self.stats[s]['person']:
                found = True
                break
        return found

    def check_references(self):
        """Check internal references within BODS data"""
        print("Checking internal references with BODS data")
        for reference in self.references:
            self.perform_check(self.check_reference(reference),
                               f"BODS referencing error: Statement {reference} not found in input data")

    def output_duplicates(self, message):
        for d in self.stats:
            if d > 1:
                for s in self.stats[d]['ownership'] | self.stats[d]['entity'] | self.stats[d]['person']:
                    self.error_log.append(f"{message} ({s})")

    def check_stats(self):
        """Check statistics for data"""
        self.perform_check(len(self.stats) == 1 and next(iter(self.stats)) == 1,
                           "BODS duplicate error: Duplicate statementIDs in input data",
                           extra_errors=self.output_duplicates)
        self.check_references()

    def error_stats(self):
        """Generate error statistics"""
        stats = {"missing": 0, "duplicate": 0, "reference": 0}
        for error in self.error_log:
            if error.startswith("Missing"): stats["missing"] += 1
            elif error.startswith("BODS duplicate"): stats["duplicate"] += 1
            elif error.startswith("BODS referencing"): stats["reference"] += 1
        return stats

    def process_errors(self):
        """Check for any errors in log"""
        for error in self.error_log:
            self.console.print(error, style="red")
        if len(self.error_log) > 0:
            stats = self.error_stats()
            estats = []
            for e in stats:
                if stats[e] > 0: estats.append(f"{stats[e]} {e}")
            estats = ", ".join(estats)
            if len(self.error_log) < 5:
                examples = ", ".join(self.error_log)
            else:
                examples = ", ".join(self.error_log[:5])
            estats += f" ({examples})"
            message = f"Consistency checks failed: {estats}"
            raise AssertionError(message)

    def run(self):
        """Run consistency checks"""
        self.read_data()
        self.generate_stats()
        self.check_stats()
        self.process_errors()
