from mrjob.job import MRJob
from mrjob.job import MRStep


class BestSellingProduct(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_extract_quantity,
                combiner=self.combiner_sum_quantity,
                reducer=self.reducer_sum_quantity
            ),
            MRStep(
                reducer=self.reducer_max_quantity
            )
        ]

    def mapper_extract_quantity(self, _, line):
        """
        This mapper reads each line from dataset and splits the features of each line and put them in an array.
        it yields the product ID (Stock code in dataset) and its corresponding quantity
        :param _:
        :param line: one line of the dataset
        :return: product ID, quantity
        """
        line_array = line.split(',')
        productID = line_array[1]
        if line_array[3].isnumeric():  # Quantity   some values in quantity column are not digits
            quantity = line_array[3]
            yield productID, float(quantity)

    def combiner_sum_quantity(self, productID, quantity):
        """
        this combiner sums all the quantities we have seen so far. So for each product ID we have the total
        number of quantity

        :param productID:
        :param quantity:
        :return: product ID, total quantity
        """
        yield productID, sum(quantity)

    def reducer_sum_quantity(self, productID, totalQuantity):
        """
        this reducer sends all (product ID,sum(quantity)) to the final reducer

        :param productID:
        :param totalQuantity:
        :return: None, total quantity, product ID
        """
        yield None, (sum(totalQuantity), productID)

    def reducer_max_quantity(self, _, totalQuantity_productID__pair):
        """
        this final reducer generates the best selling product ID and its corresponding quantity
        :param _:
        :param totalQuantity_productID__pair:
        :return: total quantity, product ID
        """
        yield max(totalQuantity_productID__pair)


if __name__ == '__main__':
    BestSellingProduct.run()
