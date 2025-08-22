from mrjob.job import MRJob

class GOCount(MRJob):

    # Mapper function
    def mapper(self, _, line):
        # Ignore metadata lines
        if not line.startswith('!'):
            columns = line.strip().split('\t')  # Split the row into columns
            if len(columns) > 4:  # Ensure there is a GO ID column
                go_id = columns[4]  # Extract the GO ID
                if go_id == "GO:0030420":  # Check if it's the target term
                    yield go_id, 1  # Emit key-value pair

    # Reducer function
    def reducer(self, key, values):
        yield key, sum(values)  # Sum up counts for each GO term

if __name__ == "__main__":
    GOCount.run()
