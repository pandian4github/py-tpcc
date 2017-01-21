# -*- coding: utf-8 -*-
# copyright (C) 2017
# Pandian Raju
# pandian@cs.utexas.edu
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


import hyperdex.admin
import hyperdex.client

import os
import logging
import commands
import uuid
from pprint import pprint,pformat
import constants

from abstractdriver import *
from pyparsing import tableName
## ==============================================
## AbstractDriver
## ==============================================
class HyperdexDriver(AbstractDriver):

    DEFAULT_CONFIG = {
        "hostname": ("The host address to the Hyperdex database","127.0.0.1"),
        "port": ("Port number",2019),
        "name": ("Name","tpcc"),
        "tolerance":("NumFailuresToTolerate", 1),
        "partitions": ("NumPartitionsPerSpace", 8)
    }



    def __init__(self, ddl):
        super(HyperdexDriver,self).__init__("hyperdex",ddl)
#         self.conn = None
        
        self.admin = None
        self.client = None
        
        self.name = "hyperdex"
#         self.database = None

#         self.new_orderspace= None
#         self.ordersspace= None
#         self.order_linespace= None
#         self.customerspace = None
#         self.warehousespace = None
#         self.districtspace = None
#         self.historyspace = None
#         self.stockspace = None
#         self.itemspace = None
        
    def makeDefaultConfig(self):
        return HyperdexDriver.DEFAULT_CONFIG

    def loadConfig(self, config):
        for key in HyperdexDriver.DEFAULT_CONFIG.keys():
            assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)        
        
        self.admin = hyperdex.admin.Admin(str(config["hostname"]), config["port"])
        self.client = hyperdex.client.Client(str(config["hostname"]), config["port"])
        
        self.admin.add_space('''
        space WAREHOUSE
        key W_ID
        attributes
            string W_ID,
            string W_NAME,
            string W_STREET_1, 
            string W_STREET_2,
            string W_CITY,
            string W_STATE,
            string W_ZIP,
            string W_TAX,
            string W_YTD
        create 8 partitions
        tolerate 1 failure
        ''')
        
        self.admin.add_space('''
        space DISTRICT
        key D_W_ID_D_ID
        attributes
            string D_ID,
            string D_W_ID,
            string D_NAME,
            string D_STREET_1, 
            string D_STREET_2, 
            string D_CITY, 
            string D_STATE,
            string D_ZIP, 
            string D_TAX,
            string D_YTD,
            string D_NEXT_O_ID
        create 8 partitions
        tolerate 1 failure
        ''')
        
        self.admin.add_space('''
        space ITEM
        key I_ID
        attributes
            string I_ID,
            string I_IM_ID,
            string I_NAME,
            string I_PRICE,
            string I_DATA
        create 8 partitions
        tolerate 1 failure
        ''')
        
        self.admin.add_space('''
        space CUSTOMER
        key C_W_ID_C_D_ID_C_ID
        attributes
            string C_ID,
            string C_D_ID,
            string C_W_ID,
            string C_FIRST,
            string C_MIDDLE,
            string C_LAST,
            string C_STREET_1,
            string C_STREET_2,
            string C_CITY,
            string C_STATE,
            string C_ZIP,
            string C_PHONE,
            string C_SINCE,
            string C_CREDIT,
            string C_CREDIT_LIM,
            string C_DISCOUNT,
            string C_BALANCE,
            string C_YTD_PAYMENT,
            string C_PAYMENT_CNT,
            string C_DELIVERY_CNT,
            string C_DATA
        create 8 partitions
        tolerate 1 failure
        ''')
        
        self.admin.add_space('''
        space HISTORY
        key U_U_ID
        attributes
            string H_C_ID,
            string H_C_D_ID,
            string H_C_W_ID,
            string H_D_ID,
            string H_W_ID,
            string H_DATE,
            string H_AMOUNT,
            string H_DATA
        create 8 partitions
        tolerate 1 failure
        ''')
        
        self.admin.add_space('''
        space STOCK
        key S_W_ID_S_I_ID
        attributes
            string S_I_ID,
            string S_W_ID,
            string S_QUANTITY,
            string S_DIST_01,
            string S_DIST_02,
            string S_DIST_03,
            string S_DIST_04,
            string S_DIST_05,
            string S_DIST_06,
            string S_DIST_07,
            string S_DIST_08,
            string S_DIST_09,
            string S_DIST_10,
            string S_YTD,
            string S_ORDER_CNT,
            string S_REMOTE_CNT,
            string S_DATA
        create 8 partitions
        tolerate 1 failure
        ''')
        
        self.admin.add_space('''
        space ORDERS
        key O_W_ID_O_D_ID_O_ID
        attributes
            string O_ID,
            string O_C_ID,
            string O_D_ID,
            string O_W_ID,
            string O_ENTRY_D,
            string O_CARRIER_ID,
            string O_OL_CNT,
            string O_ALL_LOCAL
        create 8 partitions
        tolerate 1 failure
        ''')
        
        self.admin.add_space('''
        space NEW_ORDER
        key NO_D_ID_NO_W_ID_NO_O_ID
        attributes
            string NO_O_ID,
            string NO_D_ID,
            string NO_W_ID
        create 8 partitions
        tolerate 1 failure
        ''')
        
        self.admin.add_space('''
        space ORDER_LINE
        key OL_W_ID_OL_D_ID_OL_O_ID_OL_NUMBER
        attributes
            string OL_O_ID,
            string OL_D_ID,
            string OL_W_ID,
            string OL_NUMBER,
            string OL_I_ID,
            string OL_SUPPLY_W_ID,
            string OL_DELIVERY_D
            string OL_QUANTITY,
            string OL_AMOUNT,
            string OL_DIST_INFO
        create 8 partitions
        tolerate 1 failure
        ''')

    def loadTuples(self, tableName, tuples):
        if len(tuples) == 0:
             return
        logging.debug("loading")

        if tableName == 'ITEM':
            for row in tuples:
                key = str(row[0]).zfill(5)
                i_id = str(row[0])
                i_im_id = str(row[1])
                i_name = str(row[2])
                i_price = str(row[3])
                i_data = str(row[4])
                self.client.put(tableName, key, {'I_ID':i_id, 'I_IM_ID':i_im_id, 'I_NAME':i_name, 'I_PRICE':i_price, 'I_DATA':i_data})
        if tableName == 'WAREHOUSE':
             if len(tuples[0])!=9: return
             for row in tuples: 
                 key = str(row[0]).zfill(5)  #w_ID 
                 w_id = str(row[0])
                 w_name =str(row[1])
                 w_street_1 = str(row[2])
                 w_street_2 = str(row[3])
                 w_city = str(row[4])
                 w_state = str(row[5])
                 w_zip = str(row[6])
                 w_tax = str(row[7])
                 w_ytd = str(row[8])
                 self.client.put(tableName, key, {'W_ID':w_id, 'W_NAME':w_name, 'W_STREET_1': w_street_1, 'W_STREET_2': w_street_2, 'W_CITY':w_city, 'W_STATE':w_state, 'W_ZIP':w_zip, 'W_TAX':w_tax, 'W_YTD':w_ytd})
        if tableName == 'CUSTOMER':
            for row in tuples:
                key = str(row[0]).zfill(5)+ str(row[1]).zfill(5)+ str(row[2]).zfill(5)
                c_id = str(row[0])
                c_d_id =str(row[1])
                c_w_id =str(row[2])
                c_first =str(row[3])
                c_middle = str(row[4])
                c_last = str(row[5])
                c_street_1 = str(row[6])
                c_street_2 = str(row[7])
                c_city = str(row[8])
                c_state = str(row[9])
                c_zip = str(row[10])
                c_phone = str(row[11])
                c_since = str(row[12])
                c_credit = str(row[13])
                c_credit_lim = str(row[14])
                c_discount = str(row[15])
                c_balance = str(row[16])
                c_ytd_payment = str(row[17])
                c_payment_cnt = str(row[18])
                c_delivery_cnt = str(row[19])
                c_data = str(row[20])
                self.client.put(tableName, key, {'C_ID':c_id, 'C_D_ID':c_d_id, 'C_W_ID':c_w_id, 'C_FIRST':c_first, 'C_MIDDLE':c_middle, 'C_LAST':c_last, 'C_STREET_1':c_street_1,'C_STREET_2':c_street_2, 'C_CITY':c_city, 'C_STATE':c_state, 'C_ZIP':c_zip, 'C_PHONE':c_phone, 'C_SINCE':c_since, 'C_CREDIT':c_credit, 'C_CREDIT_LIM':c_credit_lim, 'C_DISCOUNT':c_discount, 'C_BALANCE':c_balance, 'C_YTD_PAYMENT':c_ytd_payment, 'C_PAYMENT_CNT':c_payment_cnt, 'C_DELIVERY_CNT':c_delivery_cnt, 'C_DATA':c_data})                

        if tableName == 'ORDERS':
            for row in tuples:
                key = str(row[0]).zfill(5)+str(row[1]).zfill(5)+ str(row[2]).zfill(5)
                o_id = str(row[0])
                o_d_id = str(row[1])
                o_w_id = str(row[2])
                o_c_id = str(row[3])
                o_entry_d = str(row[4])
                o_carrier_id = str(row[5])
                o_ol_cnt = str(row[6])
                o_all_local = str(row[7])
                self.client.put(tableName, key, {'O_ID':o_id, 'O_D_ID':o_d_id, 'O_W_ID':o_w_id, 'O_C_ID':o_c_id, 'O_ENTRY_D':o_entry_d, 'O_CARRIER_ID':o_carrier_id, 'O_OL_CNT':o_ol_cnt, 'O_ALL_LOCAL':o_all_local})


        if tableName == 'STOCK':
            for row in tuples:
                key = str(row[0]).zfill(5)+str(row[1]).zfill(5)
                s_i_id = str(row[0])
                s_w_id = str(row[1])
                s_quantity = str(row[2])
                s_dist_01 = str(row[3])
                s_dist_02 = str(row[4])
                s_dist_03 = str(row[5])
                s_dist_04 = str(row[6])
                s_dist_05 = str(row[7])
                s_dist_06 = str(row[8])
                s_dist_07 = str(row[9])
                s_dist_08 = str(row[10])
                s_dist_09 = str(row[11])
                s_dist_10 = str(row[12])
                s_ytd = str(row[13])
                s_order_cnt = str(row[14])
                s_remote_cnt = str(row[15])
                s_data = str(row[16])
                self.client.put(tableName, key, {'S_I_ID':s_i_id, 'S_W_ID':s_w_id, 'S_QUANTITY':s_quantity, 'S_DIST_01':s_dist_01,'S_DIST_02':s_dist_02,'S_DIST_03':s_dist_03,'S_DIST_04':s_dist_04,'S_DIST_05':s_dist_05,'S_DIST_06':s_dist_06,'S_DIST_07':s_dist_07,'S_DIST_08':s_dist_08,'S_DIST_09':s_dist_09,'S_DIST_10':s_dist_10, 'S_YTD': s_ytd, 'S_ORDER_CNT':s_order_cnt, 'S_REMOTE_CNT':s_remote_cnt, 'S_DATA':s_data})
        
        if tableName == 'DISTRICT':
            for row in tuples:
                key = str(row[0]).zfill(5)+str(row[1]).zfill(5)
                d_id = str(row[0])
                d_w_id = str(row[1])
                d_name = str(row[2])
                d_street_1 = str(row[3])
                d_street_2 = str(row[4])
                d_city = str(row[5])
                d_state = str(row[6])
                d_zip = str(row[7])
                d_tax =str(row[8])
                d_ytd = str(row[9])
                d_next_o_id = str(row[10])
                self.client.put(tableName, key, {'D_ID':d_id, 'D_W_ID':d_w_id, 'D_NAME':d_name, 'D_STREET_1':d_street_1, 'D_STREET_2':d_street_2,'D_CITY':d_city, 'D_STATE':d_state, 'D_ZIP':d_zip, 'D_TAX':d_tax, 'D_YTD':d_ytd, 'D_NEXT_O_ID':d_next_o_id})
                
        if tableName == 'NEW_ORDER':
            for row in tuples:
                key = str(row[0]).zfill(5)+str(row[1]).zfill(5)+str(row[2]).zfill(5)
                no_o_id = str(row[0])
                no_d_id = str(row[1])
                no_w_id = str(row[2])
                self.client.put(tableName, key, {'NO_O_ID':no_o_id, 'NO_D_ID':no_d_id, 'NO_W_ID':no_w_id})
        
        if tableName == 'ORDER_LINE':
            for row in tuples:
                key = str(row[0]).zfill(5)+str(row[1]).zfill(5)+str(row[2]).zfill(5)+str(row[3]).zfill(5)
                ol_o_id = str(row[0])
                ol_d_id = str(row[1])
                ol_w_id = str(row[2])
                ol_number = str(row[3])
                ol_i_id = str(row[4])
                ol_supply_w_id = str(row[5])
                ol_delivery_d = str(row[6])
                ol_quantity = str(row[7])
                ol_amount = str(row[8])
                ol_dist_info = str(row[9])
                self.client.put(tableName, key, {'OL_O_ID':ol_o_id, 'OL_D_ID':ol_d_id, 'OL_W_ID':ol_w_id, 'OL_NUMBER':ol_number, 'OL_I_ID':ol_i_id, 'OL_SUPPLY_W_ID':ol_supply_w_id, 'OL_DELIVERY_D': ol_delivery_d, 'OL_QUANTITY':ol_quantity,'OL_AMOUNT':ol_amount, 'OL_DIST_INFO':ol_dist_info})
        
        if tableName == 'HISTORY':
            for i in range(len(tuples)):
                #row_key = str(i)
                key = str(uuid.uuid1())
                h_c_id = str(tuples[i][0])
                h_c_d_id = str(tuples[i][1])
                h_c_w_id = str(tuples[i][2])
                h_d_id = str(tuples[i][3])
                h_w_id = str(tuples[i][4])
                h_date = str(tuples[i][5])
                h_amount = str(tuples[i][6])
                h_data = str(tuples[i][7])
                self.client.put(tableName, key, {'H_C_ID':h_c_id, 'H_C_D_ID':h_c_d_id, 'H_C_W_ID':h_c_w_id, 'H_D_ID':h_d_id, 'H_W_ID':h_w_id, 'H_DATE':h_date,'H_AMOUNT':h_amount, 'H_DATA':h_data})
#   print tableName+'--' + str(len(tuples))
                 
    def loadFinish(self):
         logging.info("Commiting changes to database")


    ##-----------------------------------
    ## doDelivery
    ##----------------------------------
    def doDelivery(self, params):
        logging.debug("do delivery")    

        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = params["ol_delivery_d"]
        
        
        
        result = [ ]
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):
            flag=0
            for x in self.client.search('NEW_ORDER', {'NO_D_ID': str(d_id), 'NO_W_ID': str(w_id)}):
                no_o_id = x['NO_O_ID']
                flag = 1
            
            if flag == 0:
                continue
            if int(no_o_id) <= -1:
                continue
            if no_o_id == None:
                continue
    #        print no_o_id
    #        print d_id
    #        print w_id
            orders_key = no_o_id.zfill(5) + str(d_id).zfill(5) + str(w_id).zfill(5)
            #print orders_rowkey
            o = self.client.get('ORDERS', orders_key)
#             o=self.orderscf.get(orders_rowkey)
            
            c_id = str(o['O_C_ID'])
        
            ol_total = 0
            for x in self.client.search('ORDER_LINE', {'OL_O_ID': str(no_o_id), 'OL_D_ID': str(d_id),'OL_W_ID': str(w_id)}):
                ol_total += float(x['OL_AMOUNT'])
                
            deleteKey = no_o_id.zfill(5) + str(d_id).zfill(5) + str(w_id).zfill(5)
            self.client.delete('NEW_ORDER', deletekey)
            self.client.put('ORDERS', deletekey, {'O_CARRIER_ID': str(o_carrier_id)})
            self.client.put('ORDER_LINE', deletekey, {'OL_DELIVERY_D': str(ol_delivery_d)})
            
            c = self.client.get('CUSTOMER', str(c_id).zfill(5) + str(d_id).zfill(5) + str(w_id).zfill(5))
            old_balance=float(c['C_BALANCE'])
            new_balance=str(old_balance + ol_total)
            self.client.put('CUSTOMER', str(c_id).zfill(5) + str(d_id).zfill(5) + str(w_id).zfill(5), {'C_BALANCE': str(new_balance)})
                
            result.append((str(d_id), str(no_o_id)))
        ##for
        
        return result
    ##-----------------------------------
    ## doNewOrder
    ##-----------------------------------

    def doNewOrder(self, params):
        logging.debug("do new order")
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        o_entry_d = params["o_entry_d"]
        i_ids = params["i_ids"]
        i_w_ids = params["i_w_ids"]
        i_qtys = params["i_qtys"]        


        assert len(i_ids) > 0
        assert len(i_ids) == len(i_w_ids)
        assert len(i_ids) == len(i_qtys)

        all_local = True
        items = [ ]
        for i in range(len(i_ids)):
            all_local = all_local and i_w_ids[i] == w_id
            ol_i_id = i_ids[i]
#             itm=self.itemcf.get(str(ol_i_id).zfill(5), columns=['I_PRICE','I_NAME','I_DATA'])
            itm = self.client.get('ITEM', str(ol_i_id).zfill(5))
            items.append(itm)
        assert len(items) == len(i_ids)
            
        for itm in items:
            if len(itm) == 0:
                return

        #getWarehouseTaxRate
        w_tax_c = self.client.get('WAREHOUSE', str(w_id).zfill(5))
#         w_tax_c = self.warehousecf.get(str(w_id).zfill(5),columns=['W_TAX'])
        w_tax =float(w_tax_c['W_TAX'])
        #getDistrict
        
        key = str(d_id).zfill(5) + str(w_id).zfill(5)
        o = self.client.get('DISTRICT', key) 
#         o = self.districtcf.get(row_key, columns=['D_TAX','D_NEXT_O_ID'])
        d_tax = float(o['D_TAX'])
        #incrementNextOrderId
        d_next_o_id = int(o['D_NEXT_O_ID'])
            
        #getCustomer
        key = str(c_id).zfill(5) + str(d_id).zfill(5) + str(w_id).zfill(5)
        customer_info = self.client.get('CUSTOMER', key)
#         customer_info = self.customercf.get(row_key,columns=['C_DISCOUNT','C_LAST','C_CREDIT'])
        c_discount = float(customer_info['C_DISCOUNT'])
        
        o_carrier_id = constants.NULL_CARRIER_ID
        ol_cnt = len(i_ids)
    
        #incrementNextOrderId
        key = str(d_id).zfill(5) + str(w_id).zfill(5)
        self.client.put('DISTRICT', key, {'D_NEXT_O_ID': str(d_next_o_id + 1)})
#         self.districtcf.insert(row_key,{'D_NEXT_O_ID':str(d_next_o_id+1)})
            
        #createOrder
        
        order_key = str(d_next_o_id).zfill(5) + str(d_id).zfill(5) + str(w_id).zfill(5)
    #    print "d_next_o_id " +str(d_next_o_id) 
    #    print "d_id "+str(d_id)
    #    print "order_rowkey " + order_rowkey
        self.client.put('ORDERS', order_key, {'O_ID': str(d_next_o_id), 'O_D_ID': str(d_id), 'O_W_ID': str(w_id), 'O_C_ID': str(c_id), 'O_ENTRY_D': str(o_entry_d), 'O_CARRIER_ID': str(o_carrier_id), 'O_OL_CNT': str(ol_cnt), 'O_ALL_LOCAL': str(all_local)})
#         self.orderscf.insert(order_rowkey,{'O_ID':str(d_next_o_id), 'O_D_ID':str(d_id), 'O_W_ID':str(w_id), 'O_C_ID':str(c_id), 'O_ENTRY_D':str(o_entry_d), 'O_CARRIER_ID':str(o_carrier_id), 'O_OL_CNT':str(ol_cnt), 'O_ALL_LOCAL':str(all_local)})
            
        #createNewOrder
        neworder_key=str(d_next_o_id).zfill(5) + str(d_id).zfill(5) + str(w_id).zfill(5)
    #    print 'neworder_rowkey ' + neworder_rowkey
        self.client.put('NEW_ORDER', neworder_key, {'NO_O_ID': str(d_next_o_id), 'NO_D_ID': str(d_id), 'NO_W_ID': str(w_id)})
#         self.new_ordercf.insert(neworder_rowkey, {'NO_O_ID':str(d_next_o_id), 'NO_D_ID':str(d_id), 'NO_W_ID':str(w_id)})
        #getItemInfo
        total = 0
        item_data = [ ]
        for i in range(len(i_ids)):
            itemInfo = items[i]
            i_name = itemInfo['I_NAME']
            i_data = itemInfo['I_DATA']
            i_price =float(itemInfo['I_PRICE'])

        #"getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?", # d_id, ol_i_id, ol_supply_w_id
            ol_i_id = i_ids[i]
            ol_number  = i+1
            ol_supply_w_id = i_w_ids[i]
            ol_quantity = i_qtys[i]
    
            stockInfo = self.client.get('STOCK', str(ol_i_id).zfill(5) + str(ol_supply_w_id).zfill(5))
#             stockInfo = self.stockcf.get(str(ol_i_id).zfill(5)+str(ol_supply_w_id).zfill(5))
        #"updateStock": "UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?", # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
            if len(stockInfo)==0:
                 logging.warn("No STOCK record for (ol_i_id=%d, ol_supply_w_id=%d)" % (ol_i_id, ol_supply_w_id))
                 continue
            s_quantity = int(stockInfo['S_QUANTITY'])
            s_ytd = int(stockInfo['S_YTD'])
            s_order_cnt = int(stockInfo['S_ORDER_CNT'])
            s_remote_cnt = int(stockInfo['S_REMOTE_CNT'])
            s_data = stockInfo['S_DATA']
            if d_id < 10:
                s_dist_col='S_DIST_'+'0'+str(d_id)
            else:
                s_dist_col='S_DIST_'+str(d_id)
            s_dist_xx = stockInfo[s_dist_col]
            
                
            ## Update stock
            s_ytd += ol_quantity
            if s_quantity >= ol_quantity + 10:
                s_quantity = s_quantity - ol_quantity
            else:
                s_quantity = s_quantity + 91 - ol_quantity
            s_order_cnt += 1
            if ol_supply_w_id != w_id: s_remote_cnt += 1
            self.client.put('STOCK', str(ol_i_id).zfill(5) + str(ol_supply_w_id).zfill(5), {'S_QUANTITY': str(s_quantity), 'S_YTD': str(s_ytd), 'S_ORDER_CNT': str(s_order_cnt) , 'S_REMOTE_CNT': str(s_remote_cnt)})
#             self.stockcf.insert(str(ol_i_id).zfill(5)+str(ol_supply_w_id).zfill(5),{'S_QUANTITY':str(s_quantity), 'S_YTD':str(s_ytd), 'S_ORDER_CNT':str(s_order_cnt) , 'S_REMOTE_CNT':str(s_remote_cnt)})

            ##"createOrderLine": "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info
            if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING)!= -1:
                brand_generic = 'B'
            else:
                brand_generic = 'G'
            ol_amount = ol_quantity * i_price
            total += ol_amount

            orderline_key=str(d_next_o_id).zfill(5)+str(d_id).zfill(5)+str(w_id).zfill(5)
            self.client.put('ORDER_LINE', orderline_key, {'OL_O_ID': str(d_next_o_id), 'OL_D_ID': str(d_id), 'OL_W_ID': str(w_id), 'OL_NUMBER': str(ol_number), 'OL_I_ID': str(ol_i_id), 'OL_SUPPLY_W_ID': str(ol_supply_w_id), 'OL_DELIVERY_D': str(o_entry_d), 'OL_QUANTITY': str(ol_quantity),'OL_AMOUNT': str(ol_amount), 'OL_DIST_INFO': str(s_dist_xx)})
#             self.order_linecf.insert(orderline_rowkey,{'OL_O_ID': str(d_next_o_id), 'OL_D_ID':str(d_id), 'OL_W_ID':str(w_id), 'OL_NUMBER':str(ol_number), 'OL_I_ID':str(ol_i_id), 'OL_SUPPLY_W_ID':str(ol_supply_w_id), 'OL_DELIVERY_D': str(o_entry_d), 'OL_QUANTITY':str(ol_quantity),'OL_AMOUNT':str(ol_amount), 'OL_DIST_INFO':str(s_dist_xx)})
            item_data.append( (i_name, s_quantity, brand_generic,i_price, ol_amount) )
        total *= (1 - c_discount) * (1 + w_tax + d_tax)
        misc = [ (w_tax, d_tax, d_next_o_id, total) ]
        return [ customer_info, misc, item_data ]
    
    
    ##----------------------------
    ## doPayment
    ##----------------------------

    def doPayment(self, params):
        logging.debug("do payment")
        w_id = params["w_id"]
        d_id = params["d_id"]
        h_amount = params["h_amount"]
        c_w_id = params["c_w_id"]
        c_d_id = params["c_d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        h_date = params["h_date"]

        
        if c_id != None:
            #getCustomerByCustomerId
            key = str(c_id).zfill(5) + str(d_id).zfill(5) + str(w_id).zfill(5)
            customer = self.client.get('CUSTOMER', key)
#             customer = self.customercf.get(row_key)
            assert len(customer) > 0
            c_balance = float(str(customer['C_BALANCE']))- h_amount
            c_ytd_payment = float(str(customer['C_YTD_PAYMENT'])) + h_amount
            c_payment_cnt = int(str(customer['C_PAYMENT_CNT']))+1
            c_data = str(customer['C_DATA'])
            c_credit = str(customer['C_CREDIT'])
        else:
            #getCustomerByLastName
            namecnt=0
            firstnames=[]
            for x in self.client.search('CUSTOMER', {'C_W_ID': str(w_id), 'C_D_ID': str(d_id), 'C_LAST': str(c_last)}):
                firstnames.append(x['C_FIRST'])
                namecnt += 1
                if namecnt >= 1000:
                    break

#             c_1_expr = create_index_expression('C_W_ID',str(w_id))
#             c_2_expr = create_index_expression('C_D_ID',str(d_id))
#             c_3_expr = create_index_expression('C_LAST',str(c_last))
#             clause = create_index_clause([c_1_expr,c_2_expr,c_3_expr],count=1000)
            
#             newcustomer = self.customercf.get_indexed_slices(clause)
            
#             for key, column in newcustomer:
#                 firstnames.append(column['C_FIRST'])
#                 namecnt+=1
        #    print namecnt
            index = (namecnt-1)/2
            firstname = firstnames[index]
            
            for x in self.client.search('CUSTOMER', {'C_W_ID': str(w_id), 'C_D_ID': str(d_id), 'C_LAST': str(c_last)}):
                c_id = x['C_ID']
                c_balance = float(x['C_BALANCE'])- h_amount
                c_ytd_payment = float(x['C_YTD_PAYMENT']) + h_amount
                c_payment_cnt = int(x['C_PAYMENT_CNT'])+1
                c_data = x['C_DATA']
                c_credit = x['C_CREDIT']
                break
            key = str(c_id).zfill(5) + str(d_id).zfill(5) + str(w_id).zfill(5)
            customer = self.client.get('CUSTOMER', key)
        
        warehouse = self.client.get('WAREHOUSE', str(w_id).zfill(5))
        district = self.client.get('DISTRICT', str(d_id).zfill(5) + str(w_id).zfill(5))
        
#             c_4_expr = create_index_expression('C_LAST',str(c_last))
#             clause = create_index_clause([c_1_expr,c_2_expr,c_3_expr,c_4_expr],count=1)
#             newcustomer = self.customercf.get_indexed_slices(clause)
#             for key, column in newcustomer:
#                 c_id = column['C_ID']
#                 c_balance = float(column['C_BALANCE'])- h_amount
#                 c_ytd_payment = float(column['C_YTD_PAYMENT']) + h_amount
#                 c_payment_cnt = int(column['C_PAYMENT_CNT'])+1
#                 c_data = column['C_DATA']
#                 c_credit =column['C_CREDIT']
#             row_key = str(c_id).zfill(5) +str(d_id).zfill(5)+str(w_id).zfill(5)
#             customer = self.customercf.get(row_key)
#         warehouse = self.warehousecf.get(str(w_id).zfill(5))
#         district = self.districtcf.get(str(d_id).zfill(5)+str(w_id).zfill(5))

        self.client.put('WAREHOUSE', str(w_id).zfill(5), {'W_YTD': str(float(warehouse['W_YTD']) + h_amount)})
#         self.warehousecf.insert(str(w_id).zfill(5),{'W_YTD':str(float(warehouse['W_YTD'])+h_amount)})
        
        self.client.put('DISTRICT', str(d_id).zfill(5) + str(w_id).zfill(5), {'D_YTD': str(float(district['D_YTD']) + h_amount)})
#         self.districtcf.insert(str(d_id).zfill(5)+str(w_id).zfill(5),{'D_YTD': str(float(district['D_YTD'])+h_amount)})
        
        if c_credit == constants.BAD_CREDIT:
            newData = " ".join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
            c_data = (newData + "|" + c_data)
            if len(c_data) > constants.MAX_C_DATA: c_data = c_data[:constants.MAX_C_DATA]
            self.client.put('CUSTOMER', str(c_id).zfill(5) + str(c_d_id).zfill(5) + str(c_w_id).zfill(5),{'C_BALANCE': str(c_balance), 'C_YTD_PAYMENT': str(c_ytd_payment), 'C_PAYMENT_CNT': str(c_payment_cnt), 'C_DATA': str(c_data)})
#             self.customercf.insert(str(c_id).zfill(5)+str(c_d_id).zfill(5)+str(c_w_id).zfill(5),{ 'C_BALANCE' : str(c_balance), 'C_YTD_PAYMENT':str(c_ytd_payment) , 'C_PAYMENT_CNT':str(c_payment_cnt), 'C_DATA' : str(c_data)})
        else:
            c_data = ""
            self.client.put('CUSTOMER', str(c_id).zfill(5) + str(c_d_id).zfill(5) + str(c_w_id).zfill(5), {'C_BALANCE': str(c_balance), 'C_YTD_PAYMENT': str(c_ytd_payment), 'C_PAYMENT_CNT': str(c_payment_cnt)})
#             self.customercf.insert(str(c_id).zfill(5)+str(c_d_id).zfill(5)+str(c_w_id).zfill(5),{ 'C_BALANCE' : str(c_balance), 'C_YTD_PAYMENT':str(c_ytd_payment) , 'C_PAYMENT_CNT':str(c_payment_cnt)})        
        h_data= "%s    %s" % (warehouse['W_NAME'], district['D_NAME'])
        self.client.put('HISTORY', str(uuid.uuid1()), {'H_C_ID': str(c_id), 'H_C_D_ID': str(c_d_id), 'H_C_W_ID': str(c_w_id), 'H_D_ID': str(d_id), 'H_W_ID': str(w_id), 'H_DATE': str(h_date), 'H_AMOUNT': str(h_amount), 'H_DATA': str(h_data)})
#         self.historycf.insert(str(uuid.uuid1()), {'H_C_ID':str(c_id), 'H_C_D_ID':str(c_d_id), 'H_C_W_ID':str(c_w_id), 'H_D_ID':str(d_id), 'H_W_ID':str(w_id), 'H_DATE':str(h_date),'H_AMOUNT':str(h_amount), 'H_DATA':str(h_data)})
        return [warehouse, district, customer]

    
    ##-----------------------------------
    ## doOrderStatus
    ##-----------------------------------
    def doOrderStatus(self, params):
        logging.info("do orderStatus")
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]

        assert w_id, pformat(params)
        assert d_id, pformat(params)
        

        if c_id == None:            
            first_names=[ ]
            c_ids=[]
            namecnt=0
            for x in self.client.search('CUSTOMER', {'C_LAST': str(c_last), 'C_D_ID': str(d_id), 'C_W_ID': str(w_id)}):
                first_names.append(x['C_FIRST'])
                c_ids.append(x['C_ID'])
                namecnt = namecnt+1
                if namecnt >= 10000:
                    break
                            
#             last_expr = create_index_expression('C_LAST',str(c_last))
#             did_expr = create_index_expression('C_D_ID',str(d_id))
#             wid_expr = create_index_expression('C_W_ID',str(w_id))
#             clause = create_index_clause([last_expr,did_expr,wid_expr],count=10000)
#             all_customers=self.customercf.get_indexed_slices(clause)
#             for key, column in all_customers:
#                 first_names.append(column['C_FIRST'])
#                 c_ids.append(column['C_ID'])
#                 namecnt = namecnt+1
            namecnt = len(first_names)
            assert namecnt > 0
            index = (namecnt-1)/2
            first_name = first_names[index]
            assert first_name != None
            c_id = c_ids[index]
            assert c_id != None

        key1=str(c_id).zfill(5) + str(d_id).zfill(5) + str(w_id).zfill(5)
        res1 = self.client.get('CUSTOMER', key1)
#         res1=self.customercf.get(key1)
        customer = [res1['C_ID'], res1['C_FIRST'], res1['C_MIDDLE'], res1['C_LAST'], res1['C_BALANCE']]

        last_order_oid=0
        order=[]
        cnt = 0
        for x in self.client.search('ORDERS', {'O_C_ID': str(c_id), 'O_D_ID': str(d_id), 'O_W_ID': str(w_id)}):
            if int(x['O_ID']) > last_order_oid:
                last_order_oid = int(x['O_ID'])
            cnt += 1
            if cnt >= 100000:
                break 
            
        
#         cid_expr = create_index_expression('O_C_ID',str(c_id))
#         did_expr = create_index_expression('O_D_ID',str(d_id))
#         wid_expr = create_index_expression('O_W_ID',str(w_id))
#         clause = create_index_clause([cid_expr,did_expr,wid_expr],count=100000)
#         all_orders=self.orderscf.get_indexed_slices(clause)
#         
#         for key, column in all_orders:
#             if int(column['O_ID'])>last_order_oid:
#                 last_order_oid=int(column['O_ID'])
        if last_order_oid > 0:
            o = self.client.get('ORDERS', str(last_order_oid).zfill(5) + str(d_id).zfill(5) + str(w_id).zfill(5))
#             o=self.orderscf.get(str(last_order_oid).zfill(5)+str(d_id).zfill(5)+str(w_id).zfill(5))
            order = [o['O_ID'], o['O_CARRIER_ID'], o['O_ENTRY_D']]
        
        orderLines = []
        if last_order_oid>0:
            for x in self.client.search('ORDER_LINE', {'OL_O_ID': str(last_order_oid), 'OL_D_ID': str(d_id), 'OL_W_ID': str(w_id)}):
                orderLines.append([x['OL_SUPPLY_W_ID'], x['OL_I_ID'], x['OL_QUANTITY'], x['OL_AMOUNT'], x['OL_DELIVERY_D']])
#             oid_expr = create_index_expression('OL_O_ID',str(last_order_oid))
#             did_expr = create_index_expression('OL_D_ID',str(d_id))
#             wid_expr = create_index_expression('OL_W_ID',str(w_id))
#             clause = create_index_clause([oid_expr,did_expr,wid_expr])
#             orderLine=self.order_linecf.get_indexed_slices(clause)
#             for key, column in orderLine:
#                 orderLines.append([column['OL_SUPPLY_W_ID'],column['OL_I_ID'],column['OL_QUANTITY'],column['OL_AMOUNT'],column['OL_DELIVERY_D']])

        return [ customer, order, orderLines ]

        ##----------------------------
    ## doStockLevel
    ##----------------------------


    def doStockLevel(self, params):
        logging.info("do stocklevel")
        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]

    
        #"getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?", 
        d = self.client.get('DISTRICT', str(d_id).zfill(5) + str(w_id).zfill(5))
#         d = self.districtcf.get(str(d_id).zfill(5)+str(w_id).zfill(5),columns=['D_NEXT_O_ID'])
        assert d
        #getStockCount
        o_id = d['D_NEXT_O_ID']
    
        count = 0
        for x in self.client.search('STOCK', {'S_QUANTITY': ('0', str(threshold)), 'S_W_ID': str(w_id)}):
            for y in self.client.search('ORDER_LINE', {'OL_W_ID': str(w_id), 'OL_D_ID': str(d_id), 'OL_O_ID': hyperdex.client.LessEqual(str(o_id)), 'OL_O_ID': hyperdex.client.GreaterEqual(str(int(o_id) - 20))}):
                tmp1 =  x['S_I_ID']
                s_i_id = int(tmp1)
                tmp2 = y['OL_I_ID']
                ol_i_id = int(tmp2)
                if s_i_id == ol_i_id:
                    count = count + 1
        
#         s_q_expr = create_index_expression('S_QUANTITY',str(threshold), LT)
#         s_q_expr2 = create_index_expression('S_W_ID',str(w_id))
#         clause = create_index_clause([s_q_expr,s_q_expr2])
#         newstock = self.stockcf.get_indexed_slices(clause)
# 
# 
#         ol_expr = create_index_expression('OL_W_ID',str(w_id))
#         ol_expr2 = create_index_expression('OL_D_ID',str(d_id))
#         ol_expr3 = create_index_expression('OL_O_ID',str(o_id),LT)
#         ol_expr4 = create_index_expression('OL_O_ID', str(int(o_id)-20),GTE)
#         clause2 = create_index_clause([ol_expr,ol_expr2])
#         neworderline = self.order_linecf.get_indexed_slices(clause2)
#         
#         for key, column in newstock:
#             for key2, column2 in neworderline:
#                 tmp1 =  column['S_I_ID']
#                 s_i_id = int(tmp1)
#                 tmp2 = column2['OL_I_ID']
#                 ol_i_id = int(tmp2)
#                 if s_i_id == ol_i_id:
#                     count= count+1
        
        return count                

