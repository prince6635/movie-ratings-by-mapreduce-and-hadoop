# calculate total amount that's spent by each customer
# fields: customer_id, order_number, amount
# command in Canopy (nagivate to the project root first): !python total_amount_spent_by_customer.py ./assets/data/customer-orders.csv > ./target/results.txt

from mrjob.job import MRJob

class MRTotalAmountByCustomer(MRJob):

    def mapper(self, _, line):
        (customerID, orderID, orderAmount) = line.split(',')
        yield customerID, float(orderAmount)

    def reducer(self, customerID, orders):
        yield customerID, sum(orders)

if __name__ == '__main__':
    MRTotalAmountByCustomer.run()
