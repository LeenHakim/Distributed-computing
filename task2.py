from mrjob.job import MRJob
from mrjob.job import MRStep
import re

from nltk.corpus import stopwords

stopwords_english = stopwords.words('english')
stopwords_french = stopwords.words('french')

WORD_EX = re.compile(r"[\w']+")


class most15KeyWordEachGenre(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_count_words,
                combiner=self.combiner_sum_counts,
                reducer=self.reducer_sum_genre_word_count),
            MRStep(
                reducer=self.reducer_sort_count),
             MRStep(reducer=self.reducer_most_common_words)

        ]

    def mapper_count_words(self, _, line):
        """
        this function will read each line from input dataset and pars it. it checks whether the title type is
        'movie'. if so it extract words from primary title and yields the genre of the movie and extracted word
        as key and one as value
        :param _:
        :param line: each line of the input dataset
        :return: (genre,word),1 : returns the word in primary title and corresponding genre as key. one as value.
        """
        line_array = line.split('\t')
        titleType = line_array[1]
        primaryTitle = line_array[2]
        genre = line_array[8]
        if titleType == 'movie':
            words = WORD_EX.findall(primaryTitle)
            for word in words:
                yield (genre.lower(), word.lower()), 1

    def combiner_sum_counts(self, genre_word_pair, count):
        """
        This combiner obtains genre and words tuples from mapper and checks if they contains stopwords. if (genre,word)
        tuples are not stop words, it sums all pairs we have seen so far. at the end for each (genre,word) we have its
        corresponding frequency.
        :param genre_word_pair: (genre,word) : the extracted word from primary title and its corresponding genre.
        :param count: one for each pair has been read in the mapper
        :return: sums occurrence of words and its genre.
        """
        check_english = any(item in genre_word_pair for item in stopwords_english)
        check_french = any(item in genre_word_pair for item in stopwords_french)
        if not check_english and not check_french:
            yield genre_word_pair, sum(count)

    def reducer_sum_genre_word_count(self, genre_word, count):
        """
        This reducer sends all (count,(genre,word)) to next reducer
        :param genre_word: (genre,word) pair
        :param count: count of each word in primary title
        :return: (None,(counts,genre,word))
        """

        yield None, (sum(count), genre_word[0], genre_word[1])

    def reducer_sort_count(self, _, value):
        """
        this reducer reads the value from previous reducer. the value is formed as triple value like (count,genre,word).
        it returns the genre as key and (count,word) as value.
        :param _:
        :param value: it looks like a triple with following fields:
        word: each word extracted from primary title
        genre: genre of movie where the word is extracted
        count: frequency of occurrence for the word
        :return:
        """
        for count, genre, word in sorted(value, reverse=True):
            yield genre, ('%04d' % int(count), word)

    def reducer_most_common_words(self, genre, count_word):
        """
        this reducer reorders the words and returns words in the same genre exactly next to each other. it also gives
        most 15 common words.
        :param genre:
        :param count_word:
        :return: genre,(count,word)
        """
        i = 0
        for count, word in (count_word):
            i += 1
            if i <= 15:
                yield genre, (count, word)


if __name__ == '__main__':
    most15KeyWordEachGenre.run()
