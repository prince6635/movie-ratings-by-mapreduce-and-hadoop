# sorted from the least spent customer to the most
# command in Canopy (nagivate to the project root first): !python total_amount_spent_by_customer_sorted.py ./assets/data/customer-orders.csv > ./target/results.txt

# '%04.02f'%float(orderTotal)

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRTotalAmountByCustomer(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_amounts,
                   reducer=self.reducer_sum_amounts),
            MRStep(mapper=self.mapper_make_amounts_key,
                   reducer=self.reducer_output_amount)
        ]

    def mapper_get_amounts(self, _, line):
        (customerID, orderID, orderAmount) = line.split(',')
        yield customerID, float(orderAmount)

    def reducer_sum_amounts(self, customerID, orders):
        yield customerID, sum(orders)

    def mapper_make_amounts_key(self, customerID, totalAmount):
        yield '%04.02f'%float(totalAmount), customerID

    def reducer_output_amount(self, amount, customers):
        for customer in customers:
            yield amount, customer

if __name__ == '__main__':
    MRTotalAmountByCustomer.run()
