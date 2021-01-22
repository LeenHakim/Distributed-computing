from mrjob.job import MRJob
from mrjob.job import MRStep
import json
from mrjob.protocol import JSONValueProtocol
import random
from sklearn.feature_extraction.text import TfidfVectorizer

TfidfVec = TfidfVectorizer(stop_words='english')


def generate_random_summary(num=5):
    """
    this function reveives the original input json file and pick a random summary in order to be compared with
    other summaries in other papers
    :param num:
    :return: summary of randomly picked paper.
    """
    path='C:\\Pycharm\\DC_project\\ProjectDCSA2021\\3_TEXT-SIMILARITY\\arxivData_2.json'
    json_file = open(path)

    data = json.load(json_file)
    summary_list = []
    for i in data["scientific_papers"]:
        summary_list.append(i["summary"])
    length = len(summary_list)
    random_num = random.randint(0, length)
    return summary_list[random_num]


def cos_similarity(doc1, doc2):
    """
    this function implements the calculation of cosine similarity between two documents
    :param doc1: first document
    :param doc2: second document
    :return: cosine score which represents the relevance between two documents
    """
    docs = [doc1, doc2]
    tfidf = TfidfVec.fit_transform(docs)
    tfidf_matrix = (tfidf * tfidf.T).toarray()
    return tfidf_matrix[0][1]


global summary
summary = generate_random_summary()


class Cosin_similarity(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_calculate_cosine_similarity,
                   reducer=self.reducer_get_cosine_similarity),
            MRStep(reducer=self.reducer_find_most_relevant_summaries)
        ]

    INPUT_PROTOCOL = JSONValueProtocol

    #random_num = generate_random_summary()

    def mapper_calculate_cosine_similarity(self, _, line):  # output.jsonl as input file
        """
        This function calculates the cosine similarity score between the randomly picked summary and all
        other summaries from the dataset.
        :param _:
        :param line: input line of dataset
        :return: (random summary,other summaries),cosine score
        """
        yield (summary, line["summary"]), cos_similarity(summary, line["summary"])


    def reducer_get_cosine_similarity(self, summary_pair, cosine_score):
        """
        This function sends all (cosine_score, picked summary, other summary) to the final reducer
        in order to sort them based on the cosine similarity score
        :param summary_pair: (random summary,other summary) tuples
        :param cosine_score: cosine score between the randomly picked summary and other summaries
        :return: None,(cosine_score,random summary,other summary)
        """
        yield None, (sum(cosine_score), summary_pair[0], summary_pair[1])


    def reducer_find_most_relevant_summaries(self, _, value):
        """
        This function gives the 3 most relevant papers to our randomly picked paper
        :param _:
        :param value: (cosine_score, random summary,other summary)
        :return: top 3 most relevant summaries and their cosine score
        """
        i = 0
        for score, random_summary, other_summary in sorted(value, reverse=True):
            i += 1
            if i <= 3:
                yield '%02.06f' % float(score), (random_summary, other_summary)


if __name__ == '__main__':
    Cosin_similarity.run()
