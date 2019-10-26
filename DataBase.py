# -*- coding: utf-8 -*-
"""
Created on Wed Oct 23 01:34:16 2019

@author: Lizerhigh
"""
import os
import pandas
import matplotlib.pyplot as plt

class DBError(Exception):
    def __init__(self, code, text):
        super().__init__(text)
        self._code = code
        self._text = text

class Column(str):
    def __init__(self, name):
        try:
            if type(name) == Column:
                self._name = name._name[::]
                self._vtype = name._vtype[::]
            else:
                name = name.replace(' ', '')
                self._name = name.split(':')[0]
                self._vtype = name.split(':')[1]
        except IndexError:
            raise DBError(-7, 'Error while parsing command')
        
    def __str__(self):
        return f'{self._name}: {self._vtype}'
    
    def __getitem__(self, value):
        return (eval(self._vtype))(value)
    
    def __eq__(self, another):
        if type(another) == str:
            return (self._name == another)
        else:
            return (self._name == another._name) 
        
    def __repr__(self):
        return f'{self._name}: {self._vtype}'
    
    def __hash__(self):
        return self._name.__hash__()
    

class DataBase:
    global _backs
    _backs = '\\'
    
    def __init__(self):
        self._commands = [x for x in dir(self) if not x.startswith('_')]
    
    def USE(self, arg):
        if not os.path.isfile(arg):
            arg = f'{os.getcwd()}{_backs if not arg.startswith(_backs) else ""}{arg}{".db" if not arg.endswith(".db") else ""}'
            if os.path.isfile(arg):
                return self.USE(arg)
            else:
                raise DBError(-1, 'Database file not found')
        self._file = arg
        self._table = pandas.read_json(self._file)
        self._table.columns = list(map(Column, self._table.columns))
        return 0
        
    def APPLY(self, file = ''):
        if file:
            self._file = file
        tmp = self._table.copy()
        tmp.columns = list(map(str, tmp.columns))
        json = tmp.to_json()
        f = open(self._file, 'w')
        f.write(json)
        f.close()
        return 0
        
    def SHOW(self, n = None):
        try:
            tmp = self._table.copy()
            if n:
                n = int(n)
                tmp = tmp.head(n) if n > 0 else tmp.tail(-n)
            tmp.columns = [x._name for x in tmp.columns]
            tmp.index = ['']*len(tmp.index)
            return tmp
        except:
            raise DBError(-5, 'No database loaded! Load db with "USE <database_name>" or create new one using "CREATE <db_name> [columns]"')
    
    def PLOT(self, args):
        try:
            y_axis = args.split()[0]
            x_axis = None
            if len(args.split())>1:
                x_axis = args.split()[1]
                if x_axis == 'index':
                    x_axis = None
        except:
            raise DBError(-7, 'Error while parsing command')
        
        try:
            tmp = self._table.copy()
            tmp.columns = [x._name for x in tmp.columns]
            plt.plot(tmp[x_axis] if x_axis else range(len(self)), tmp[y_axis], label=y_axis)
            plt.legend()
            plt.show()
            return 0
        except DBError:
            raise DBError(-5, 'No database loaded! Load db with "USE <database_name>" or create new one using "CREATE <db_name> [columns]"')

    def CREATE(self, args):
        arg = args.split()[0]
        self._file = f'{os.getcwd()}{_backs if not arg.startswith(_backs) else ""}{arg}{".db" if not arg.endswith(".db") else ""}'
        if os.path.isfile(self._file):
            raise DBError(-2, 'Database with this name already exists')
        columns = None
        if len(args.split()) > 1:
            columns = list(map(Column, DataBase._remove_es(args.split(' ', 1)[1].replace(', ', ',').replace(': ', ':')).split(',')))
        self._table = pandas.DataFrame(columns = columns)
        return self.APPLY()
    
    def SELECT(self, args):
        try:
            args = args.split()
            args = ' '.join(['WHERE' if x.upper() == 'WHERE' else x for x in args])
            query = args.split(' WHERE ')[0].replace(', ', ',').split(',')
            query = [self._table.columns[x] for x in [list(self._table.columns).index(i) for i in query]]
            condition = DataBase._format_cond(args.split(' WHERE ')[1]) if ' WHERE ' in args else ''
            ret = pandas.DataFrame(columns = query)
            c = 0
        except:
            raise DBError(-7, 'Error while parsing command')
        for j in self._table.index:
            if (condition) and (DataBase._check_cond(self._table.loc[j], condition)):
                ret.loc[c] = [self._table.loc[j][x] for x in query]
                c += 1
        ret.columns = [x._name for x in ret.columns]
        return ret
    
    def INSERT(self, args):
        args = args.split()
        args = ' '.join(['VALUES' if x.upper() == 'VALUES' else x for x in args])
        values = args.split(' VALUES ')[1].replace(', ', ',').split(',')
        columns = args.split(' VALUES ')[0].replace(', ', ',').split(',')
        values = [values[columns.index(x._name)] if x._name in columns else None for x in self._table.columns]
        self._table.loc[len(self)] = values
        return 0
    
    def DROP(self):
        try:
            s = '1'
            while (s)and(s.lower() != 'yes')and(s.lower() != 'y')and(s.lower() != 'n')and(s.lower() != 'no'):
                s = input(f'Do you really want to delete "{self._file.split(_backs)[-1].split(".")[0]}"? This action can\'t be undone! (Y/[N]): ')
            if (not s)or(s.lower().startswith('n')):
                return 1
            os.remove(self._file)
            self._file = None
            self._table = None
            return 0
        except:
            raise DBError(-5, 'No database loaded! Load db with "USE <database_name>" or create new one using "CREATE <db_name> [columns]"')
    
    def EXIT(self):
        raise DBError(0, 'Goodbye!')
    
    def __call__(self, command):
        command = DataBase._remove_es(command)
        p = command.split()[0].upper()
        if not p in self._commands:
            raise DBError(-3, 'Command not found! Try "HELP" to see command line options')
        try:
            return eval(f'self.{p}')(command.split(' ', 1)[1]) if len(command.split()) > 1 else eval(f'self.{p}')()
        except TypeError:
            raise DBError(-7, 'Error while parsing command')
    def __len__(self):
        return len(self._table)
    
    def _remove_es(s):
        while '  ' in s:
            s = s.replace('  ', ' ')
        return s
    
    def _format_cond(s):
        return DataBase._remove_es(s.replace('(', ' ( ').replace(')', ' ) ').replace('.', ' .').replace('=', ' = ').replace('<', ' < ').replace('>', ' > ').replace(' <  = ', ' <= ').replace(' >  = ', ' >= ').replace(' =  = ', ' == '))
        
    def _format_back(s):
        return s.replace(' ( ', '(').replace(' ) ', ')').replace(' .', '.').replace('( ', '(').replace(') ', ')')
    
    def _check_cond(args, condition):
        try:
            c = condition.split()
            for i in args.keys():
                #print(i[args[i]])
                c = [x if x != i._name else i[args[i]].__repr__() for x in c]
            exp = DataBase._format_back(' '.join(c))
            #print(exp)
            return eval(exp)
        except:
            raise DBError(-6, 'Condition error')

def example():        
    db = DataBase()
    ret = 0
    err = DBError(1, 'no error')
    while err._code != 0:
        try:
            s = ''
            while not s.replace(' ', '').replace('\t', ''):
                s = input('DB>')
            ret = db(s)
            #print(type(ret))
            if (type(ret) != int):
                if (type(ret) == pandas.DataFrame)and(ret.empty):
                    print('Nothing found')
                else:
                    print(ret)
            elif ret == 0:
                print('Success!')
            elif ret == 1:
                print('Cancelled')
        except DBError as e:
            print(e._text)
            err = e
            
if __name__=='__main__':
    example()
