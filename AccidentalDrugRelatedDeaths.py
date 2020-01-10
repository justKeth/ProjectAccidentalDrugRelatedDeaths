"""                                               პ რ ო ე ქ ტ ი !!!
პროექტისთვის სხვადასხვა ტიპის მონაცემთა ბაზის ფაილების მოპოვება შესაბამისი აღწერებით შესაძლებელია https://catalog.data.gov/dataset მისამართზე. პროექტის ფარგლებში შესაძლებელია ჩამოტვირთოთ ფაილი csv, json ან xlsx გაფართოებით.
პროექტისადმი მოთხოვნები შემდეგია:
  პროექტს ასრულებს 1 სტუდენტი
  პროექტის ჩაბარება მოხდება შესაბამის ლაბორატორიულ მეცადინეობაზე 26 და 28 დეკემბერს ჯგუფების მიხედვით
  კოდი უნდა იყოს დაწერილი ობიექტზე ორიენტირებული მიდგომით (4 ქულა);
  panda მოდულის გამოყენებით უნდა ხდებოდეს შესაბამისი ფაილიდან მონაცემების წამოღება (2 ქულა);
  უნდა ხდებოდეს thread-ების გამოყენება მონაცემების დამუშავების პროცესში (2 ქულა);
  დამუშავების შემდეგ მონაცემები უნდა იქნას შენახული SQLite3 მონაცემთა ბაზაში (2 ქულა);
  უნდა იყოს შესაძლებელი მიღებული შედეგების ვიზუალიზაცია matplotlib მოდულის გამოყენებით (2 ქულა);
  უნდა იყოს გათვალისწინებლი სხვადასხვა ტიპის შეცდომების დამუშავება, რომელიც რეალურად შეიძლება წარმოიქმნას პროექტის ფარგლებში
  კოდი უნდა იყოს კარგად დაკომენტირებული, რომ ადამიანს შეეძლოს ადვილად მისი გარჩევა. ასეთ კოდს მივიღებ პრეზენტაციად (4 ქულა).
  ამოცანის დასმა და პრეზენტაციის გაკეთება (მოყოლა) (3 ქულა). დრო 5 - 7 წუთი პროექტზე."""

import os
import sqlite3  # database
import threading
import matplotlib.pyplot as plt
import pandas


class AccidentalDrugRelatedDeaths:
    sqlite3_table_name = "AccidentalDrugRelatedDeaths"
    sqlite3_path = os.getcwd() + "\\" + 'Sqlite.db'

    def __init__(self, ind):  # constructor
        self.ID = ind[0]
        self.date = ind[1]
        self.dateType = ind[2]
        self.age = ind[3]
        self.sex = ind[4]
        self.race = ind[5]
        self.residenceCity = ind[6]
        self.residenceCounty = ind[7]
        self.residenceState = ind[8]
        self.deathCity = ind[9]
        self.deathCounty = ind[10]
        self.location = ind[11]
        self.locationifOther = ind[12]
        self.descriptionofInjury = ind[13]
        self.injuryPlace = ind[14]
        self.injuryCity = ind[15]
        self.injuryCounty = ind[16]
        self.injuryState = ind[17]
        self.COD = ind[18]
        self.otherSignifican = ind[19]

    def visualise_data(self):
        sqlite3_name = self.sqlite3_path
        connection = None

        try:
            connection = sqlite3.connect(sqlite3_name)  # აქ ვუკავშირდებით ბაზას
            print(f'{sqlite3_name} ბაზის ფაილს წარმატებით დავუკავშირდით!')

            data = pandas.read_sql('select * from AccidentalDrugRelatedDeaths limit 10;',
                                   connection)  # ვკითხულობთ 10 მონაცემს

            data.plot(kind='bar', x='date', y='otherSignifican', color='black')  # ვიზუალიზაცია
            plt.show()

        except sqlite3.Error as err:
            print(
                f'ბაზასთან დაკავშირებისას დაფიქსირდა შეცდომა: {err}')  # შეცდომის დაფიქსირების შემთხვევაში ვაგდებთ exceptions

        finally:
            if connection:  # თუკი ბაზასთან ჯერ კიდევ დაკავშირებულები ვართ
                connection.close()  # გავწყვიტოთ კავშირი

    def writeData(self, w):
        sqlite3_name = self.sqlite3_path
        connection = None

        try:
            connection = sqlite3.connect(sqlite3_name)  # აქ ვუკავშირდებით ბაზას
            print(f'{sqlite3_name} ბაზის ფაილს წარმატებით დავუკავშირდით!')

            cur = connection.cursor()  # თუ უკვე არსებობს ამ სახელით მაშინ ვშლით
            cur.execute('''DROP IF EXISTS AccidentalDrugRelatedDeaths''')

            w.to_sql(name=self.sqlite3_table_name, con=connection)
            print("მონაცემები ჩაიწერა!")

        except sqlite3.Error as err:
            print(
                f'ბაზასთან დაკავშირებისას დაფიქსირდა შეცდომა: {err}')  # შეცდომის დაფიქსირების შემთხვევაში ვაგდებთ exceptions

        finally:
            if connection:  # თუკი ბაზასთან ჯერ კიდევ დაკავშირებულები ვართ
                connection.close()  # გავწყვიტოთ კავშირი


def read_data():
    data = pandas.read_csv('AccidentalDrugRelatedDeaths.csv')
    f = pandas.DataFrame(data,
                         columns=['ID', "date", "dateType", "age", "sex", "race", "residenceCity", "residenceCounty",
                                  "residenceState", "deathCity", "deathCounty", "location", "locationifOther",
                                  "descriptionofInjury", "injuryPlace", "injuryCity", "injuryCounty", "injuryState",
                                  "COD", "otherSignifican"])
    f = pandas.to_numeric(f["otherSignifican"])
    return f  # აბრუნებს DataFrame-ს


if __name__ == '__main__':
    data = read_data()
    obj = AccidentalDrugRelatedDeaths(list(data))
    obj.writeData(data)

    t1 = threading.Thread(target=obj.visualise_data(), args=("thread-1",))
    t2 = threading.Thread(target=obj.visualise_data(), args=("thread-2",))

    t1.start()
    t1.join()
    t2.start()
    t2.join()