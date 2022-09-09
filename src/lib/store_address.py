import random
import uuid 
from faker import Faker 
import datetime


class StoreAddress():
  
  def __init__(self):
    self.fake = Faker()
    
  

  def create_store_address(self, sid):
    """
    When given a store id, this will generate address information for the store 
    """
    aid = str(uuid.uuid4())

    data = {
      'address_id': aid,
      'store_id': sid,
      'address': self.fake.street_address(),
      'city': self.fake.city(),
      'state': self.fake.state(),
      'zip_code': self.fake.postcode(), 
      'created_date': str(datetime.datetime.utcnow()), 
      'created_by': 'system'
    }
    return data
  
