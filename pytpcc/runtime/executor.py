# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http://www.cs.brown.edu/~pavlo/
#
# Original Java Version:
# Copyright (C) 2008
# Evan Jones
# Massachusetts Institute of Technology
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------

import logging
from datetime import datetime

import constants
from util import *

class Executor:
    
    def __init__(self, handle, parameters):
        self.handle = handle
        self.params = parameters
    ## DEF
    
    def execute(self, duration):
        logging.info("Executing benchmark for %d seconds" % duration)
        results = Results(handle)
        start = results.start()

        while (datetime.now() - start).seconds <= duration:
            txn, params = self.doOne()
            logging.info("Executing '%s' transaction" % txn)
            self.handle.executeTransaction(txn, params)
            results.completed(txn)
        ## WHILE
            
        results.stop()
        return (results)
    ## DEF
    
    def doOne(self):
        """Selects and executes a transaction at random. The number of new order transactions executed per minute is the official "tpmC" metric. See TPC-C 5.4.2 (page 71)."""
        
        ## This is not strictly accurate: The requirement is for certain
        ## *minimum* percentages to be maintained. This is close to the right
        ## thing, but not precisely correct. See TPC-C 5.2.4 (page 68).
        x = rand.number(1, 100)
        params = None
        txn = None
        if x <= 4: ## 4%
            txn, params = (constants.TransactionTypes.STOCK_LEVEL, doStockLevel())
        elif x <= 4 + 4: ## 4%
            txn, params = (constants.TransactionTypes.DELIVERY, doDelivery())
        elif x <= 4 + 4 + 4: ## 4%
            txn, params = (constants.TransactionTypes.ORDER_STATUS, doOrderStatus())
        elif x <= 43 + 4 + 4 + 4: ## 43%
            txn, params = (constants.TransactionTypes.PAYMENT, doPayment())
        else: ## 45%
            assert x > 100 - 45
            txn, params = (constants.TransactionTypes.NEW_ORDER, doNewOrder())
        
        return (txn, params)
    ## DEF
    
    def doStockLevel(self):
        """Returns parameters for STOCK_LEVEL"""
        w_id = makeWarehouseId(self.parameters)
        d_id = makeDistrictId(self.parameters)
        threshold = rand.number(constants.MIN_STOCK_LEVEL_THRESHOLD, constants.MAX_STOCK_LEVEL_THRESHOLD)
        return makeParameterDict(locals(), "w_id", "d_id", "threshold")
    ## DEF

    def doOrderStatus(self):
        """Return parameters for ORDER_STATUS"""
        w_id = makeWarehouseId(self.parameters)
        d_id = makeDistrictId(self.parameters)
        c_last = None
        c_id = None
        
        ## 60%: order status by last name
        if rand.number(1, 100) <= 60:
            c_last = rand.makeRandomLastName(self.params.customersPerDistrict)

        ## 40%: order status by id
        else:
            c_id = makeCustomerId(self.parameters)
            
        return makeParameterDict(locals(), "w_id", "d_id", "c_id", "c_last")
    ## DEF

    def doDelivery(self):
        """Return parameters for DELIVERY"""
        w_id = makeWarehouseId(self.parameters)
        o_carrier_id = rand.number(constants.MIN_CARRIER_ID, constants.MAX_CARRIER_ID)
        ol_delivery_d = datetime.now()
        return makeParameterDict(locals(), "w_id", "carrier", "ol_delivery_d")
    ## DEF

    def doPayment(self):
        """Return parameters for PAYMENT"""
        x = rand.number(1, 100)
        y = rand.number(1, 100)

        w_id = makeWarehouseId(self.parameters)
        d_id = makeDistrictId(self.parameters)
        c_w_id = None
        c_d_id = None
        c_id = None
        c_last = None
        h_amount = rand.fixedPoint(2, constants.MIN_PAYMENT, constants.MAX_PAYMENT)
        h_date = datetime.now()

        ## 85%: paying through own warehouse (or there is only 1 warehouse)
        if self.params.warehouses == 1 or x <= 85:
            c_w_id = w_id
            c_d_id = d_id
        ## 15%: paying through another warehouse:
        else:
            ## select in range [1, num_warehouses] excluding w_id
            c_w_id = rand.numberExcluding(self.params.starting_warehouse, self.params.max_w_id, w_id)
            assert c_w_id != w_id
            c_d_id = makeDistrictId(self.parameters)

        ## 60%: payment by last name
        if y <= 60:
            c_last = generator.makeRandomLastName(self.params.customersPerDistrict)
        ## 40%: payment by id
        else:
            assert y > 60
            c_id = makeCustomerId(self.parameters)

        return makeParameterDict(locals(), "w_id", "d_id", "h_amount", "c_w_id", "c_d_id", "c_id", "c_last", "h_date")
    ## DEF

    def doNewOrder(self):
        """Return parameters for NEW_ORDER"""
        w_id = makeWarehouseId(self.parameters)
        d_id = makeDistrictId(self.parameters)
        c_id = makeCustomerId(self.parameters)
        ol_cnt = rand.number(constants.MIN_OL_CNT, constants.MAX_OL_CNT)
        o_entry_d = datetime.now()

        ## 1% of transactions roll back
        rollback = (allow_rollback and rand.number(1, 100) == 1)

        i_ids = [ ]
        i_w_ids = [ ]
        i_qtys = [ ]
        for i in range(0, ol_cnt):
            if rollback and i + 1 == ol_cnt:
                i_ids.append(self.params.items + 1)
            else:
                i_ids.append(generator.makeItemId(self.parameters))

            ## 1% of items are from a remote warehouse
            remote = (rand.number(1, 100) == 1)
            if self.params.warehouses > 1 and remote:
                i_w_ids.append(rand.numberExcluding(self.params.starting_warehouse, self.params.max_w_id, w_id))
            else:
                i_w_ids.append(w_id)

            i_qtys.append(rand.number(1, constants.MAX_OL_QUANTITY))
        ## FOR

        return makeParameterDict(locals(), "w_id", "d_id", "c_id", "o_entry_d", "i_ids", "i_w_ids", "i_qtys")
    ## DEF


    def makeWarehouseId(self):
        w_id = rand.number(self.params.starting_warehouse, self.params.max_w_id)
        assert(w_id >= self.params.starting_warehouse), "Invalid W_ID: %d" % w_id
        assert(w_id <= self.params.max_w_id), "Invalid W_ID: %d" % w_id
        return w_id
    ## DEF

    def makeDistrictId(self):
        return rand.number(1, self.params.districtsPerWarehouse)
    ## DEF

    def makeCustomerId(self):
        return rand.NURand(1023, 1, self.params.customersPerDistrict)
    ## DEF

    def makeItemId(self):
        return rand.NURand(8191, 1, self.params.items)
    ## DEF

    def makeParameterDict(values, *args):
        return dict(map(lambda x: (x, values[x]), args))
    ## DEF