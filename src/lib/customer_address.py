# import libraries as needed 
import random
import uuid 
from faker import Faker 
import datetime


class CustomerAddress():
  
  def __init__(self):
    self.fake = Faker()
    
  
  def create(self, cid):
    """
    When given a customer id, this will generate address information for the customer 
    """
    aid = str(uuid.uuid4())
    
    data = {
      'address_id': aid,
      'customer_id': cid,
      'address': self.fake.street_address(),
      'city': self.fake.city(),
      'state': self.fake.state(),
      'zip_code': self.fake.postcode(), 
      'created_date': str(datetime.datetime.utcnow()), 
      'created_by': 'system'
    }
    return data
  
  
  
  def update(self, cid):
    """
    When given a customer id, this will generate address information for the customer 
    """
    aid = str(uuid.uuid4())
    
    data = {
      'address_id': aid,
      'customer_id': cid,
      'address': self.fake.street_address(),
      'city': self.fake.city(),
      'state': self.fake.state(),
      'zip_code': self.fake.postcode(), 
      'created_date': str(datetime.datetime.utcnow()), 
      'created_by': 'system'
    }
    return data
  
