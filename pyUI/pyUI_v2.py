import mysql.connector as sql
import paho.mqtt.client as mqtt
import multitasking
import threading
import sys
import time
from PyCRC.CRC16 import CRC16
from PyQt5 import QtWidgets 
from PyQt5.QtCore import Qt, QThread, pyqtSignal,QObject,pyqtSlot
from mainwindow import Ui_MainWindow
from PyQt5.QtWidgets import QApplication
listMachines = []
broker_url = ''
host = ''
user = ''
passwd = ''
database = ''

class AllSignal(QObject):

    qtyChanged = pyqtSignal(int, int)


class Machine(QObject):
   
    def __init__(self, idmachine, line, operation):
        super(QObject, self).__init__()
        self.broker_url = broker_url
        self.idMachine = idmachine
        self.line = line
        self.operation = operation
        self.onConnect = False
        self.amoutOfProducts = None
        self.countTimeDown = 0
        self.flagFirstTime = True
        self.timeDuration = 0
        self.signal1 = AllSignal()

    def joinInMqtt(self):
        self.topicMainData = 'phubai2/realtimeproduction/topicMainData/{}'.format(self.idMachine)
        self.topicCheckIDHR = 'phubai2/realtimeproduction/topicCheckIDHR/{}'.format(self.idMachine)
        self.client = mqtt.Client(str(self.idMachine))
        self.client.connect(self.broker_url, 1883)
        self.client.loop_start()

    def checkValidData(self,client, userdata, message):
        try:
            array_message = ""
            array_message = str(message.payload.decode())
            if len(array_message) != 41:
                raise Exception
            self.__crcChecksum = int(array_message[-5:])
            self.__crcChecksum_new = CRC16().calculate(str(array_message[:-5]))
            
            if (self.__crcChecksum_new != self.__crcChecksum):
                raise Exception

            self.__idMachine_mgs = str(int(array_message[1:11]))

            if (self.__idMachine_mgs != self.idMachine):
                raise Exception
            self.__amoutOfProducts_mgs = int(array_message[31:36])
            if (self.amoutOfProducts == self.__amoutOfProducts_mgs):
                raise Exception
            #print(self.__idMachine_mgs, self.idMachine)

            self.__idhr_mgs = int(array_message[11:21])
            self.__wls_mgs = int(array_message[21:31])
            #print(" CheckValidData: OK")
            #print("ID: %d\nIDHR: %d\nWls: %d" %
            #(self.__idMachine_mgs,self.__idhr_mgs, self.__wls_mgs))
            if self.amoutOfProducts == None:
                self.amoutOfProducts = self.__amoutOfProducts_mgs
                raise Exception
            self.amoutOfProducts = self.__amoutOfProducts_mgs
            self.flagFirstTime = False
            self.countTimeDown = 0
            self.timeDuration = time.time()
            self.onConnect = True
            self.insertIntoMySQL()
        except Exception:
            #print("error")
            pass
            #print(" CheckValidData: FAILED ID:%d GROUP:%d LINE:%s"
            #      %(self.idMachine, self.group, self.line))
        except:
            pass

    def insertIntoMySQL(self):
        try:
            
            #mydb = sql.connect(host=host,
            #            user=user,
            #            passwd=passwd,
            #            database=database)
            #cursor = mydb.cursor()
            insql = "insert into pj_hbi.realtime (IDMay, IDHR, LOT, SLSP) values (%s, %s, %s, %s)"
            val = (self.idMachine, self.__idhr_mgs, self.__wls_mgs, self.__amoutOfProducts_mgs)
        
            cursor.execute(insql, val)
            mydb.commit()
            print("      Machine: {}    AoP: {}".format(self.idMachine,self.__amoutOfProducts_mgs))

        except:
            pass

    def checkOnConnect(self):
        if self.countTimeDown > 300:
            self.onConnect = False 

    @pyqtSlot()
    def scanData(self):
        while True:
            self.client.subscribe(self.topicMainData)
            self.client.message_callback_add(self.topicMainData,self.checkValidData)
            time.sleep(0.1)

    @pyqtSlot()
    def scanIDNV(self):
        while True:
            self.client.subscribe(self.topicCheckIDHR)
            #self.client.message_callback_add(self.topicCheckIDHR,self.checkValidData)
            time.sleep(0.1)


class ApplicationWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super(ApplicationWindow, self).__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.ui.lineEdit_setTimeOff.setInputMask('999')
        self.signalButton()

    def signalButton(self):
        self.ui.pushButton_onConnect.clicked.connect(self.onClick_pushButton_onConnect)
        self.ui.pushButton_getList.clicked.connect(self.getListMachines)
        self.ui.pushButton_run.clicked.connect(self.onClick_runButton)
    
    @pyqtSlot()
    def onClick_pushButton_onConnect(self):
        global broker_url
        global host 
        global user
        global passwd
        global database
        broker_url = self.ui.lineEdit_ip.text()
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

    @pyqtSlot()
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

    @pyqtSlot()
    def onClick_runButton(self):
        self.ui.tableWidget_listMachines.clearContents()
        selectedGroup = self.ui.comboBox_listMachines.currentText()
        if selectedGroup is None:
            self.ui.plainTextEdit.appendPlainText('Setting isnt complete!')
            return
        self.ui.plainTextEdit.appendPlainText(F'Loading list machines of group {selectedGroup}....')
        sql_select_Query = "SELECT * FROM pj_hbi.listmachines WHERE GROUPNO='%s'" % selectedGroup
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()
        records.sort(key=lambda x: x[1])
        self.listMachines = []
        self.listThread = []
        for index_row, row in enumerate(records):
                self.listMachines.append(Machine(str(row[0]),str(row[1]),str(row[3])))
                self.listThread.append(QThread()) 
        for index_machine, machine in enumerate(self.listMachines):
            self.ui.tableWidget_listMachines.setItem(index_machine,0,QtWidgets.QTableWidgetItem(machine.idMachine))
            self.ui.tableWidget_listMachines.setItem(index_machine,1,QtWidgets.QTableWidgetItem(machine.line))
            machine.joinInMqtt()
            machine.timeDuration = time.time()
        for index_machine, machine in enumerate(self.listMachines):
            machine.moveToThread(self.listThread[index_machine])
            self.listThread[index_machine].started.connect(machine.scanData)
            self.listThread[index_machine].start()
        self.ui.plainTextEdit.appendPlainText(F'Amount of {selectedGroup} : {len(records)}')
        self.ui.plainTextEdit.appendPlainText('DONE')
        self.updateTable()

    def updateTable(self):
        while True:
            QApplication.processEvents()
            for index_machine, machine in enumerate(self.listMachines):
                if machine.flagFirstTime == False:
                    self.ui.tableWidget_listMachines.setItem(index_machine,2,QtWidgets.QTableWidgetItem(str(machine.amoutOfProducts)))
                else:
                    self.ui.tableWidget_listMachines.setItem(index_machine,2,QtWidgets.QTableWidgetItem('.....'))
                sec = int(time.time() - machine.timeDuration)
                text_time = F'{sec//3600}h {(sec//60)%60}m {sec%60}s'
                setTimeOff = self.ui.lineEdit_setTimeOff.text()
                if setTimeOff=="":
                    setTimeOff = "1"
                if sec // 60 >= int(setTimeOff):
                    self.ui.tableWidget_listMachines.setItem(index_machine,3,QtWidgets.QTableWidgetItem(str(text_time)+"    OFF"))
                else:
                    self.ui.tableWidget_listMachines.setItem(index_machine,3,QtWidgets.QTableWidgetItem(str(text_time)))
                


if __name__ == "__main__":

    app = QtWidgets.QApplication(sys.argv)
    application = ApplicationWindow()
    application.show()

    sys.exit(app.exec_())