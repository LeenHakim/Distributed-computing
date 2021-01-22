import nltk
from mrjob.job import MRJob
from mrjob.job import MRStep
import re

nltk.download('stopwords')
from nltk.corpus import stopwords

nltk.download('stopwords')
stopwords_english = stopwords.words('english')
stopwords_french = stopwords.words('french')

WORD_EX = re.compile(r"[\w']+")


class Most50keywords(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_extract_words,
                combiner=self.combiner_words_counts,
                reducer=self.reducer_sum_word_counts
            ),
            MRStep(
                reducer=self.reduce_sort_counts
            )
        ]

    def mapper_extract_words(self, _, line):
        """
        This mapper first separates those rows in which the title type is either "short" or "movie" and then
        extract word in primary title and yields the word to 1.
        :param _:
        :param line: one line from input file
        :return: (word,1)
        """
        line_array = line.split('\t')
        titleType = line_array[1]
        primaryTitle = line_array[2]
        if titleType == 'short' or titleType == 'movie':
            words = WORD_EX.findall(primaryTitle)
            for word in words:
                yield word.lower(), 1

    def combiner_words_counts(self, word, counts):
        """
        This combiner sums all the word we have seen so far. It gives us the frequency of each word extracted
        from previous mapper
        :param word: words obtained from the mapper
        :param counts: 1
        :return: sums occurrence of each word
        """
        yield word, sum(counts)

    def reducer_sum_word_counts(self, word, counts):
        """
        this reducer sends all (word,sum(count)) to the final reducer
        :param word: words obtained from the combiner
        :param counts: frequency of each word
        :return: None, (sum(counts), word)
        """
        yield None, (sum(counts), word)

    def reduce_sort_counts(self, _, word_counts):
        """
        this final reducer gets the (word,count) pair as the value and sort them and returns the most 50
        common words
        :param _:
        :param word_counts: (word,count) pair
        :return: key=count value=word
        """
        i = -1
        for count, key in sorted(word_counts, reverse=True):
            if key not in stopwords_english and key not in stopwords_french:
                i += 1
                if i == 50:
                    break
                yield '%04d' % int(count), key


if __name__ == '__main__':
    Most50keywords.run()
