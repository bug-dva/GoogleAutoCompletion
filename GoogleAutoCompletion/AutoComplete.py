from mrjob.job import MRJob
from mrjob.step import MRStep

import re

class NGramBuilder(MRJob):


    def configure_options(self):
        super(NGramBuilder,self).configure_options()
        self.add_passthrough_option('--noGram',help='The N for your N Gram is')
        self.add_passthrough_option('--threshold', help='This is the threshold set to filter data')
        self.add_passthrough_option('--TopK', help='This is the top items')

    def steps(self):
        return[MRStep(mapper=self.mapper_get_phrase, reducer=self.reducer_count_phrase),
               MRStep(mapper=self.mapper_get_pr, reducer=self.reducer_count_pr)
]

    def mapper_get_phrase(self,_,line):
        words = re.sub('[^a-zA-Z]+',' ',line).split()

        """
        for word in words:
            yield word.lower(), 1
        """
        if len(words) < 2:
            return
        for i in range(len(words)):
            sb = []
            sb.append(words[i])
            i = i + 1
            for j in range(1,len(words)-i):
                if j < int(self.options.noGram):
                    sb.append(' '.join(words[i:j+1]))
                    j = j + 1
                for keys in sb:
                    yield keys,1

    def reducer_count_phrase(self, key, values):
        yield key, sum(values)

    def mapper_get_pr(self, key, value):

    def reducer_get_pr(self, key, values):

        # treemap get top 5
        # key = i love big
        # value= [data=10, girl=100, boy=1000,baby = 10]
        # get topK
        # value: data=10
        for value in values:
            value





if __name__ == '__main__':
    NGramBuilder.run()