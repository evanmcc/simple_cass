#!/usr/bin/env python

import unittest
import simple_cass
from simple_cass import uuid
import random
from time import sleep

#only good for the local environment

def key_gen(l):
    ret = []
    while len(ret) < l:
        val = str(int(random.random()*2000+10))
        if val not in ret:
            ret.append(val)
    return ret

usr1,usr2,usr3 = key_gen(3)

class scass_test(unittest.TestCase):
    def setUp(self):
        self.ci = simple_cass.cass_con('Fictions')
        #better would be to generate this stuff at the top
        #and then remove what got added on teardown
        self.ci.remove('Users', usr1)
        self.ci.remove('Users', usr2)
        self.ci.remove('Users', usr3)
        self.ci.remove('UserRelationships', usr1)
        self.ci.remove('UserRelationships', usr3)

    # the a is to make it go first, so the test data exists 
    # for the other tests.
    def test_a_insert(self):
        assert(self.ci != None)
        self.ci.insert('Users', usr1, {'name': 'JillR', 
                                      'id': 'foobar' })

        dval = self.ci.get('Users', usr1)
        assert(dval['name'] == 'JillR')
        assert(dval['id'] == 'foobar')

        self.ci.insert('Users', usr1, {'name': 'jillr'})
        val = self.ci.get_val('Users', usr1,'name' )
        assert(val == 'jillr')
        self.ci.insert('Users', usr1, {'id': '1234'})
        val = self.ci.get_val('Users', usr1,'id' )
        assert(val == '1234')

    def test_get_val(self):
        self.ci.insert('Users', usr3, {'name': 'slothrop'})
        val = self.ci.get_val('Users', usr3,'name' )
        assert(val == 'slothrop')
  
    def test_get(self):
        self.ci.insert('Users', usr1, {'name': 'jillr',
                                       'id': '1234',
                                       'ign': 'true'})
        assert(self.ci.get('Users', usr1, 
                           ['name', 'id']) == {'name':'jillr', 
                                               'id':'1234'})
        
        assert(self.ci.get('Users', usr1) == {'name':'jillr', 
                                              'id':'1234',
                                              'ign': 'true'})
        
    def test_get_w_scols(self):
        self.ci.insert('UserRelationships', usr3, 
                       {'friends': { uuid():usr1, uuid():'3'},
                        'enemies': { uuid():usr3}}) 
        assert(self.ci.get('UserRelationships', usr1) == {})
        assert(len(self.ci.get('UserRelationships', usr3))== 2)
        
    def test_remove(self):
        self.ci.insert('Users', usr2, {'name': 'lucky', 
                                      'id': 'flarf' })
        self.ci.remove('Users', usr2, 'name')
        assert(self.ci.get('Users', usr2) == {'id':'flarf'})
        self.ci.remove('Users', usr2)
        assert(self.ci.get('Users', usr2) == {})

    def test_scol_remove(self):
        self.ci.insert('UserRelationships', usr3, 
                       {'friends': { uuid():usr1, uuid():'3'},
                        'enemies': { uuid():usr3}}) 
        assert(len(self.ci.get('UserRelationships', usr3)) == 2)
        self.ci.remove('UserRelationships', usr3, super_col='friends')
        assert(len(self.ci.get('UserRelationships', usr3)) == 1)
        self.ci.remove('UserRelationships', usr3)
        assert(self.ci.get('UserRelationships', usr3) == {})


if __name__ == '__main__':
    unittest.main()
