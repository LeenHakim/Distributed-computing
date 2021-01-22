from mrjob.job import MRJob
from mrjob.job import MRStep


class most_amount_spent_by_customer(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_extract_revenue,
                combiner=self.combiner_sum_revenue,
                reducer=self.reducer_sum_revenue
            ),
            MRStep(
                reducer=self.reducer_most_spent_revenue
            )
        ]

    def mapper_extract_revenue(self, _, line):
        """
        This mapper reads each line from dataset and splits the features of each line and put them in an array.
        it calculates the revenue for each line and it yields the customer ID and the revenue
        :param _:
        :param line: each line of the input file
        :return: customerID,revenue
        """
        line_array = line.split(',')
        if line_array[3].isnumeric():  # Quantity
            quantity = float(line_array[3])
            price = float(line_array[5])
            customerID = line_array[6]
            revenue = quantity * price
            yield customerID, float(revenue)

    def combiner_sum_revenue(self, customerID, revenue):
        """
        the combiner sums all the revenues we have seen so far. So for each costumer ID we have the total revenue
        spent
        :param customerID:
        :param revenue:
        :return: customer ID,total revenue
        """
        yield customerID, sum(revenue)

    def reducer_sum_revenue(self, customerID, totalRevenue):
        """
        this reducer sends all (customerID,sum(revenue)) to the final reducer
        :param customerID:
        :param totalRevenue:
        :return: None, (sum(revenue), customer ID)
        """
        yield None, (sum(totalRevenue), customerID)

    def reducer_most_spent_revenue(self, _, totalRevenue_customerID_pair):
        """
        this reducer has the (total revenue, customer ID) pair as value. it sorts the entities and gives the top 10 one.
        :param _:
        :param totalRevenue_customerID_pair:
        :return: top 10 (revenue,customerID)
        """
        i = -1
        for revenue, customerId in sorted(totalRevenue_customerID_pair, reverse=True):
            i += 1
            if customerId != "":
                if i <= 10:
                    yield '%04.02f' % float(revenue), customerId


if __name__ == '__main__':
    most_amount_spent_by_customer.run()
