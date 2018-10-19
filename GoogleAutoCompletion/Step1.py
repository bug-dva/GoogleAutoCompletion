from mrjob.job import MRJob

import re

class NGramBuilder(MRJob):


    def configure_options(self):
        super(NGramBuilder,self).configure_options()
        self.add_passthrough_option('--noGram',help='The N for your N Gram is')

    def mapper(self,_,line):
        words = re.sub('[^a-zA-Z]+',' ',line).strip().split()

        """
        for word in words:
            yield word.lower(), 1
        """
        if len(words) < 2:
            return
        sb=[]
        for i in range(len(words)):
            # get all str starts with word[i]
            for j in range(i+1,min(i+int(self.options.noGram),len(words))):
                sb.append(' '.join(words[i:j+1]))

        for keys in sb:
            yield keys,1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    NGramBuilder.run()