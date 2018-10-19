from mrjob.job import MRJob
import re
from heapq import heappush,heappop

class LanguageModel(MRJob):

    def configure_options(self):
        super(LanguageModel,self).configure_options()
        self.add_passthrough_option('--threshold',help='threshold for data filtering')
        self.add_passthrough_option('--topK', help='top K for output')

    def mapper(self, _, line):
        # refine line [l love big 3]
        line = re.sub('\"', '', line)

        # line.split [1,love,big,3]
        words_plus_count = line.split()

        # threshold :filter low fre pairs
        if int(words_plus_count[-1]) < int(self.options.threshold):
            return

        # get output key  ,a string, i love
        output_key = ' '.join(words_plus_count[:-2])

        # get output value ,string, big=3
        output_value = words_plus_count[-2] + '=' + words_plus_count[-1]

        yield output_key,output_value

    def reducer(self, key, values):
        # key = i love big , values= [data=10,girl=100,boy=1000]
        # get topK

        #init variable
        h = []
        # for each value, divide into word and count
        for value in values:
            word,count = value.split('=')
            # push each value into a heapq, (count,word)
            
            heappush(h,(-int(count),word))
        # pop topK count
        heappop(h)

        # output_key = key,word,count
        # output_value = None


        yield key,' '.join(values)


if __name__ == '__main__':
    LanguageModel.run()
