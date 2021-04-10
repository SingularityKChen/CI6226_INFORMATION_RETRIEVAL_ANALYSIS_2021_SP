class ranking_BM25:
    def __init__(self, f_k1, f_b, f_n):
        """

        :param f_k1:
        :param f_b:
        :param f_n: number of documents in the database
        """
        self.k = f_k1
        self.b = f_b
        self.n = f_n

    def df(self):
        """
        Compute the document frequency of the document
        :return:
        """
        pass

    def idf(self):
        """
        Compute the idf of term in the document d
        :return:
        """
        pass

    def tf_td(self):
        """
        Compute term frequency in document d
        :return: term frequency in document d
        """
        pass

    def l_d(self):
        """
        Compute the length of doc d
        :return: the length of doc d
        """
        pass

    def l_avg(self):
        """
        Compute the average doc length in collection
        :return: the average doc length in collection
        """

    def get_score(self):
        """
        Compute the BM25 score
        :return:
        """
        pass
