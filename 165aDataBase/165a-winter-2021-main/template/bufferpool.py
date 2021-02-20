from template.config import *
# from template.table import *
import io
import os
import pickle


class Bufferpool:

    def __init__(self, table):
        self.basic_path = './ECS165/' + table.name + '/'
        # self.pool = [[],[],[]]
        # LRU cache
        self.pool = {}
        # self.pool_prg_num_list = []
        self.trash_bin = {}
        # self.trash_prg_num_list = []
        self.num_cols = table.num_columns + META_DATA_COL_NUM
        self.cap = self.num_cols * MAX_PRANGE
        self.prange_num = 0
        self.load_pos = 0

        """
        NOT SURE IF SHOULD BE PLACED IN __init__ OR SOMEWHERE ELSE
        MAYBE IN BUFFERPOOL
        After open a database, for the specific table
        start the Daemon thread to clean the trashbin periodically
        @func: cleanBin - function in bufferpool to clean trashbin
        cleanMission = threading.Thread(target=cleanBin)
        cleanMission.daemon = True
        cleanMission.start()
        """
    
    # prg means prange,  pg means page
    def get_(self, col, prg_pos, mode):
        if self.pool == []:
            return 0
        if prg_pos in self.pool:
            # index = self.pool_prg_num_list.index(prg_pos)
            # print(index, col)
            if mode == 'up':
                prange_list = self.pool.pop(prg_pos)
                self.pool[prg_pos] = prange_list
            elif mode == 're':
                prange_list = self.pool[prg_pos]
            else:
                if self.pool[prg_pos][0][0].isFull == True:        
                    prange_list = self.pool[prg_pos]
                else:
                    prange_list = self.pool.pop(prg_pos)
                    self.pool[prg_pos] = prange_list
            return prange_list[col]
        elif prg_pos in self.trash_bin:
            # index = self.trash_prg_num_list.index(prg_pos)
            # print('trash index', index)
            prange_list = self.trash_bin.pop(prg_pos)
            trash_list = self.pool.pop(next(iter(self.pool)))
            self.trash_bin[trash_list[0][0].prange_id] = trash_list
            self.pool[prg_pos] = prange_list
            return prange_list[col]
            # self.trash_prg_num_list.remove(prg_pos)
            # for prange in prange_list:
            #     self.load_prange(prange[0])
            # if prg_pos in self.pool_prg_num_list:
            #     index = self.pool_prg_num_list.index(prg_pos)
            #     # print(len(self.pool[index]))
            #     prange =  self.pool[index][col]
            #     return prange
        # searching in disk
        else:
            prange_list = self.read_from_disk(prg_pos)
            if not isinstance(prange_list, int):
                if len(self.pool) == MAX_PRANGE:
                    trash_list = self.pool.pop(next(iter(self.pool))) 
                    self.move_to_trash(trash_list)
                self.pool[prg_pos] = prange_list
            return prange_list[col]
        return -1

    # no use
    # def set_(self, col, prg_pos, prange):
    #     if prg_pos in self.pool_prg_num_list:
    #         index = self.pool_prg_num_list.index(prg_pos)
    #         self.pool[index][col] = prange

    def move_to_trash(self, trash_list):
        disk_list = None
        if len(self.trash_bin) == MAX_PRANGE:
            # if next(iter(self.trash_bin))[0][0].prange_id == prg_pos:
            #     disk_list = 
            disk_list = self.trash_bin.pop(next(iter(self.trash_bin)))
            self.write_to_disk(disk_list)
        self.trash_bin[trash_list[0][0].prange_id] = trash_list
    
    def load_prange(self, prange):
        if prange == None:
            return

        if prange.prange_id in self.pool:
            self.pool[prange.prange_id].append([prange])
        else:
            if len(self.pool) == MAX_PRANGE:
                trash_list = self.pool.pop(next(iter(self.pool)))
                # self.trash_bin[prange_list[0][0].prange_id] = prange_list
                self.move_to_trash(trash_list)
            self.pool[prange.prange_id] = [[prange]]
        # # print(prange.prange_id)
        # idx = 0
        # if self.prange_num == self.cap:
        #     self.free_pool()
        # for index in range(MAX_PRANGE):
        #     # print(len(self.pool[index]), index)
        #     if len(self.pool[index]) < self.num_cols:
        #         self.pool[index].append([prange])
        #         idx = index
        #         break
        # if prange.prange_id not in self.pool_prg_num_list:
        #     self.pool_prg_num_list.insert(index, prange.prange_id)
        # self.prange_num += 1

    # no use    
    # def free_pool(self):
    #     record = []
    #     for i in range(self.num_cols):
    #         record.append(self.pool[0].pop(0))
    #         self.prange_num -= 1
    #     self.trash_bin.append(record)
    #     self.trash_prg_num_list.append(self.pool_prg_num_list.pop(0))

    def write_to_disk(self, prange_list):
        path = self.basic_path + 'Data'
        if not os.path.isdir(path):
            os.mkdir(path)
        f = open(path + '/prange' + '_' + str(prange_list[0][0].prange_id) + '.pkl', 'wb')
        pickle.dump(prange_list, f, True)
        f.close()

    def read_from_disk(self, prg_pos):
        path = self.basic_path + 'Data'
        if not os.path.isdir(path):
            print('Reading failed: No such directory')
            return -1
        f_path = path + '/prange' + '_' + str(prg_pos) + '.pkl'
        if not os.path.isfile(f_path):
            print('Reading failed: No such file')
            return -2
        print(f_path)
        f = open(f_path, 'rb+')
        prange_list = pickle.load(f)
        f.close()
        # self.load_prange(prange)
        return prange_list


    def cleanBin(self):
        for prange_list in self.pool.values():
            self.write_to_disk(prange_list)
        for prange_list in self.trash_bin.values():
            self.write_to_disk(prange_list)
        # whlie True:
            # process to clean the bin:
            # merge tail page and base page
            # time.sleep(x) # x = time interval to clean the bin.
        
