from mrjob.job import MRJob
from mrjob.job import MRStep


class BestSellingProductRevenue(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_extract_revenue,
                combiner=self.combiner_sum_revenue,
                reducer=self.reducer_sum_revenue
            ),
            MRStep(
                reducer=self.reducer_max_revenue
            )
        ]

    def mapper_extract_revenue(self, _, line):
        """
        This mapper reads each line from dataset and splits the features of each line and put them in an array.
        it yields the product ID (Stock code in dataset) and its corresponding revenue
        :param _:
        :param line:
        :return: product ID, revenue
        """
        line_array = line.split(',')
        productID = line_array[1]
        if line_array[3].isnumeric():  # Quantity   some values in quantity column are not digits
            quantity = float(line_array[3])
            price = float(line_array[5])
            revenue = quantity * price
            yield productID, float(revenue)

    def combiner_sum_revenue(self, productID, revenue):
        """
        this combiner sums all the revenues we have seen so far. So for each product ID we have the total
        number of revenues

        :param productID:
        :param revenue:
        :return: product ID, total revenue
        """
        yield productID, sum(revenue)

    def reducer_sum_revenue(self, productID, totalRevenue):
        """
        this reducer sends all (product ID,sum(revenue)) to the final reducer
        :param productID:
        :param totalRevenue:
        :return: None, total revenue, product ID
        """
        yield None, (sum(totalRevenue), productID)

    def reducer_max_revenue(self, _, totalRevenue_productID__pair):
        """
        this final reducer generates the best selling product ID and its corresponding revenue
        :param _:
        :param totalRevenue_productID__pair:
        :return: total revenue, product ID
        """
        yield max(totalRevenue_productID__pair)


if __name__ == '__main__':
    BestSellingProductRevenue.run()
