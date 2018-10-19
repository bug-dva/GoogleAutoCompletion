from mrjob.job import MRJob

class NGramBuilder(MRJob):

    #  input: I love big data\t10
    # output: key=I love big  , value=data=10

    def mapper_get_pr(self, key, value):
        line = str(value).strip().lower()
        words_plus_count = line.split('\t')
        if (len(words_plus_count) < 2 ):
            return
        words = words_plus_count[0].split()
        starting_phrase = words[:-1]
        following_word = words[-1]+'='+words_plus_count[1]
        yield starting_phrase,following_word

    def reducer_get_pr(self, key, values):
        yield key, values


if __name__ == '__main__':
    NGramBuilder.run()