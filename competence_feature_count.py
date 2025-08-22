import os
from mrjob.job import MRJob

class CompetenceFeatureCount(MRJob):
    def mapper(self, _, line):
        # Extract the 5th column (GO ID) and file name
        fields = line.strip().split('\t')
        if len(fields) > 4:
            go_id = fields[4]  # GO ID is in the 5th column
            file_name = os.environ.get('mapreduce_map_input_file')  # Get file name
            organism = os.path.basename(file_name).split('-')[0]  # Extract organism name from file name
            
            # Check for "GO:0030420" or related terms
            if go_id.startswith("GO:0030420"):
                yield organism, 1

    def reducer(self, organism, counts):
        # Sum counts for each organism
        yield organism, sum(counts)

if __name__ == '__main__':
    CompetenceFeatureCount.run()
