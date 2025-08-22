from mrjob.job import MRJob

class GAFGeneCount(MRJob):

    def mapper(self, _, line):
        # Split the line into fields (assuming tab-separated)
        fields = line.strip().split('\t')
        if len(fields) > 1:  # Ensure there's enough data
            gene_id = fields[1]  # Assuming the gene ID is in the second column
            yield gene_id, 1

    def reducer(self, key, values):
        # Sum up the occurrences for each gene ID
        yield key, sum(values)

if __name__ == '__main__':
    GAFGeneCount.run()
