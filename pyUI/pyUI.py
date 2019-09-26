import mysql.connector as sql
import paho.mqtt.client as mqtt
import multitasking
import threading
import sys
import time
from PyQt5 import QtWidgets 
from PyQt5.QtCore import Qt, QThread, pyqtSignal,QObject
from mainwindow import Ui_MainWindow

### global variable
class Machine(QObject):
    def __init__(self, idmachine, line, operation, broker_url):
        super(QObject, self).__init__()
        self.broker_url = broker_url
        self.idMachine = idmachine
        self.line = line
        self.operation = operation
        self.onConnect = False
        self.amoutOfProducts = None
        self.countTimeDown = 0

    def joinInMqtt(self):
        self.topicMainData = 'phubai2/realtimeproduction/topicMainData/{}'.format(self.idMachine)
        self.topicCheckIDHR = 'phubai2/realtimeproduction/topicCheckIDHR/{}'.format(self.idMachine)
        self.client = mqtt.Client(str(self.idMachine))
        self.client.connect(self.broker_url, 1883)
        self.client.loop_start()
        time.sleep(2)
        print(self.idMachine)

    def checkValidData(self,client, userdata, message):
        try:
            array_message = str(message.payload.decode())
            if len(array_message) != 41:
                raise Exception
            self.__crcChecksum = int(array_message[-5:])
            self.__crcChecksum_new = CRC16().calculate(str(array_message[:-5]))
            if (self.__crcChecksum_new != self.__crcChecksum):
                raise Exception
            self.__idMachine_mgs = int(array_message[1:11])
            if (self.__idMachine_mgs != self.idMachine):
                raise Exception
            self.__amoutOfProducts_mgs = int(array_message[31:36])
            if (self.amoutOfProducts == self.__amoutOfProducts_mgs):
                raise Exception
            self.__idhr_mgs = int(array_message[11:21])
            self.__wls_mgs = int(array_message[21:31])
            #print(" CheckValidData: OK")
            #print("ID: %d\nIDHR: %d\nWls: %d" %
            #(self.__idMachine_mgs,self.__idhr_mgs, self.__wls_mgs))
            self.amoutOfProducts = self.__amoutOfProducts_mgs
            self.countTimeDown = 0
            self.onConnect = True
            self.insertIntoMySQL()
        except Exception:
            pass
            #print(" CheckValidData: FAILED ID:%d GROUP:%d LINE:%s"
            #      %(self.idMachine, self.group, self.line))
        except:
            pass

    def insertIntoMySQL(self):
        try:
            cursor = mydb.cursor()
            insql = "insert into realtime (IDMay, IDHR, LOT, SLSP, OP) values (%s, %s, %s, %s, %s)"
            val = (self.idMachine, self.__idhr_mgs, self.__wls_mgs, self.__amoutOfProducts_mgs, self.operation)
            cursor.execute(insql, val)
            mydb.commit()
            print("      Machine: {}    AoP: {}".format(self.idMachine,self.__amoutOfProducts_mgs))

        except:
            pass
            #print(" Insert Into MySQL: Failed
            #Machine:{}".format(self.idMachine))

    def checkIdhr(self,client, userdata, message):
        pass

    def checkOnConnect(self):
        if self.countTimeDown > 300:
            self.onConnect = False     

class MyThread(QThread):
    # Create a counter thread
    change_value = pyqtSignal()
    change_value_2 = pyqtSignal()
    def run(self):
        self.cnt = 0
        while self.cnt < 1:
            self.cnt+=1
            time.sleep(0.3)
            self.change_value.emit()
            self.change_value_2.emit()


class ApplicationWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super(ApplicationWindow, self).__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.signalButton()
        self.thread = MyThread()
        self.thread.change_value.connect(self.table)
        self.thread.change_value_2.connect(self.table1)
        #self.thread.start()
        #self.startProgressBar()


    def signalButton(self):
        self.ui.pushButton_onConnect.clicked.connect(self.onClick_pushButton_onConnect)
        self.ui.pushButton_getList.clicked.connect(self.getListMachines)
        self.ui.pushButton_run.clicked.connect(self.onClick_runButton)

    def onClick_pushButton_onConnect(self):
        global host 
        global user
        global passwd
        global database
        self.broker_url = self.ui.lineEdit_ip.text()
        host = self.ui.lineEdit_hostDB.text()
        user = self.ui.lineEdit_userDB.text()
        passwd = self.ui.lineEdit_passwdDB.text()

        database = self.ui.lineEdit_database.text()
        try:
            global mydb
            global cursor
            mydb = sql.connect(host=host,
                        user=user,
                        passwd=passwd,
                        database=database)
            cursor = mydb.cursor()
            text = 'Connect to Database: Successfully!'
            self.ui.plainTextEdit.appendPlainText(text)
            del text
        except:
            text = 'Connect to Database: Failed!'
            self.ui.plainTextEdit.appendPlainText(text)
            del text
    
    def table(self):
        #val = str(val)
        print(str(self.thread.cnt))
        rowPosition = self.ui.tableWidget_listMachines.rowCount()
        self.ui.tableWidget_listMachines.setItem(0,1,QtWidgets.QTableWidgetItem(str(self.thread.cnt)))

    def table1(self):
        #val = str(val)
        print(str(self.thread.cnt))
        rowPosition = self.ui.tableWidget_listMachines.rowCount()
        self.ui.tableWidget_listMachines.setItem(1,1,QtWidgets.QTableWidgetItem(str(self.thread.cnt)))

    def getListMachines(self):
        try:
            self.listGroups = []
            sql_select_Query = "SELECT * FROM pj_hbi.listmachines" 
            cursor.execute(sql_select_Query)
            records = cursor.fetchall()
            self.ui.plainTextEdit.appendPlainText("Total number of rows is: {}".format(cursor.rowcount))
            for row in records:
                if row[2] not in self.listGroups:
                    self.listGroups.append(row[2])
            self.listGroups = sorted(self.listGroups)
            self.ui.comboBox_listMachines.addItems(self.listGroups)
            self.ui.plainTextEdit.appendPlainText(F'Length {len(self.listGroups)}')

        except:
            self.ui.plainTextEdit.appendPlainText('Please try to push Connect to Database firstly!')
            pass

    def onClick_runButton(self):
        self.listMachines = []
        self.listThread = []
        self.ui.tableWidget_listMachines.clear()
        selectedGroup = self.ui.comboBox_listMachines.currentText()
        print(selectedGroup)
        if selectedGroup is None:
            self.ui.plainTextEdit.appendPlainText('Setting isnt complete!')
            return
        self.ui.plainTextEdit.appendPlainText(F'Loading list machines of group {selectedGroup}....')
        #
        sql_select_Query = "SELECT * FROM pj_hbi.listmachines WHERE GROUPNO='%s'" % selectedGroup
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()
        for index_row, row in enumerate(records):
                self.listMachines.append(Machine(str(row[1]),str(row[0]),str(row[3]),self.broker_url))
                print(self.listMachines[index_row].idMachine)
                self.listThread.append(QThread()) 
        for index_machine, machine in enumerate(self.listMachines):
            self.ui.tableWidget_listMachines.setItem(index_machine,0,QtWidgets.QTableWidgetItem(machine.idMachine))
            self.ui.tableWidget_listMachines.setItem(index_machine,1,QtWidgets.QTableWidgetItem(machine.line))
            self.ui.tableWidget_listMachines.setItem(index_machine,2,QtWidgets.QTableWidgetItem(machine.amoutOfProducts))
            #self.listThread[index_machine].create(machine.joinInMqtt)
        for index_machine, machine in enumerate(self.listMachines):
            machine.moveToThread(self.listThread[index_machine])
            self.listThread[index_machine].started.connect(machine.joinInMqtt)
        for thread in self.listThread:
            thread.start()

        self.ui.plainTextEdit.appendPlainText(F'Amount of {selectedGroup} : {len(records)}')
        self.ui.plainTextEdit.appendPlainText('DONE')
        abc = self.ui.tableWidget_listMachines.indexFromItem(QtWidgets.QTableWidgetItem(951))
        self.ui.plainTextEdit.appendPlainText(str(abc.row()))
        
        
        
                                                         


    #def startProgressBar(self):
        
    #    self.thread = MyThread()
    #    self.thread.change_value.connect(self.table)
    #    self.thread.start()
if __name__ == "__main__":

    app = QtWidgets.QApplication(sys.argv)
    application = ApplicationWindow()
    application.show()

    sys.exit(app.exec_())