"""
 @Time: 2021/6/17 11:10
 @Author: CodingYoo
 @desc: 导出实时表数据到excel
"""

# 主要功能：分批次导出大数据量、结构相同的数据表到excel
# 导出多个表的数据到各自的文件，
# 目前问题：to_excel 虽然设置了分批写入，但先前的数据会被下一次写入覆盖，
# 利用Pandas包中的ExcelWriter()方法增加一个公共句柄，在写入新的数据之时保留原来写入的数据，等到把所有的数据都写进去之后关闭这个句柄
import pymysql
import pandas as pd
import math

from main import getEveryDay


class MSSQL(object):
    def __init__(self, host, user, pwd, db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db

    def __getConn(self):
        if not self.db:
            raise (NameError, '没有设置数据库信息')
        self.conn = pymysql.connect(host=self.host, user=self.user, password=self.pwd, database=self.db, charset='utf8')
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, '连接数据库失败')
        else:
            return cur

    def executeQuery(self, sql):
        cur = self.__getConn()
        cur.execute(sql)
        # 获取所有数据集
        # fetchall()获取结果集中的剩下的所有行
        # 如果数据量太大，是否需要分批插入
        resList, rowcount = cur.fetchall(), cur.rowcount
        self.conn.close()
        return (resList, rowcount)

    # 导出单个数据表到excel
    def writeToExcel(self, **args):
        sourceTB = args['sourceTB']
        columns = args.get('columns')
        path = args['path']
        fname = args['fname']
        startRow = args['startRow']
        isHeader = args['isHeader']
        N = args['batch']

        # 获取指定源数据列
        if columns is None:
            columns_select = ' * '
        else:
            columns_select = ','.join(columns)

        if fname is None:
            fname = sourceTB + '_exportData.xlsx'
        else:
            fname = fname + '_exportData.xlsx'

        file = path + fname
        # 增加一个公共句柄，写入新数据时，保留原数据
        writer = pd.ExcelWriter(file, mode='w')
        fetch_data2 = ()
        rowcount2 = 0
        for sourceTB in args['sourceTB']:
            sql_select = 'select ' + columns_select + ' from ' + sourceTB + " where readdate regexp '00$|30$' and deviceid = '949c2beb54c74e4c9011095e84086fe5' GROUP BY readdate desc"
            fetch_data, rowcount = self.executeQuery(sql_select)
            print(f"from ==========={sourceTB}============ 表查询到的条数为:" + str(rowcount))
            fetch_data2 = fetch_data + fetch_data2
            rowcount2 = rowcount + rowcount2
            print(f"总条数为:" + str(rowcount2))
        df_fetch_data = pd.DataFrame(fetch_data2)
        # 一共有roucount行数据，每N行一个batch提交写入到excel
        times = math.floor(rowcount2 / N)
        i = 1
        rs_startrow = 0
        # 当总数据量 > 每批插入的数据量时
        print(i, times)
        is_while = 0
        while i <= times:
            is_while = 1
            # 如果是首次，且指定输入标题，则有标题
            if i == 1:
                # isHeader = True
                startRow = startRow
            else:
                # isHeader = False
                startRow += N
            # 切片取指定的每个批次的数据行 ,前闭后开
            # startrow: 写入到目标文件的起始行。0表示第1行，1表示第2行。。。
            # df_fetch_data['batch'] = 'batch' + str(i)
            df_fetch_data[rs_startrow:i * N].to_excel(writer, header=isHeader, index=False, startrow=startRow)
            # writer.save()
            print('第', str(i), '次循环，取源数据第', rs_startrow, '行至', i * N, '行', '写入到第', startRow, '行')
            print('第', str(i), '次写入数据为：', df_fetch_data[rs_startrow:i * N])
            # 重新指定源数据的读取起始行
            rs_startrow = i * N
            i += 1

        # 写入文件的开始行数
        # 当没有做任何循环时，仍然从第一行开始写入
        if is_while == 0:
            startRow = startRow
        else:
            startRow += N
        # df_fetch_data['batch'] = 'batch' + str(i)
        print('第{0}次读取数据，从第{1}行开始，写入到第{2}行！'.format(str(i), str(rs_startrow), str(startRow)))
        print('第', str(i), '写入数据为：', df_fetch_data[rs_startrow:i * N])
        df_fetch_data[rs_startrow:i * N].to_excel(writer, header=isHeader, index=False, startrow=startRow)
        # 注： 这里一定要save()将数据从缓存写入磁盘！
        writer.save()

    # 导出结构相同的多个表到同一样excel
    def exportToExcel(self, **args):
        arc_dict = dict(
            sourceTB=args['sourceTB'],
            path=args['path'],
            startRow=args['startRow'],
            isHeader=args['isHeader'],
            fname=args['fname'],
            columns=args['columns'],
            batch=args['batch']
        )
        print('\n当前导出的数据表为：%s' % (args['sourceTB']))
        self.writeToExcel(**arc_dict)
        return 'success'


if __name__ == "__main__":
    ms = MSSQL(host="192.168.9.97", user="root", pwd="654321", db="energy_prod")
    # data_list = getEveryDay('2020-06-01', '2020-12-31')  # 每5个月存一张excel  2020年6月1号开始有数据
    data_list = getEveryDay('2021-06-15', '2021-06-20')
    sourceTB = []
    str_data = ''
    for i in data_list:

        str_data = 'rteq_dizb' + i
        sourceTB.append(str_data)
    args = dict(
        sourceTB=sourceTB,  # 待导出的表
        # sourceTB=['rteq_dizb20210429'],  # 待导出的表
        path='C:\\Users\\17622\\Desktop\\',  # 导出到指定路径
        startRow=1,  # 设定写入文件的首行，第2行为数据首行
        isHeader=False,  # 是否包含源数据的标题
        # fname='energy2_2020',
        fname='energy2_2021',
        columns=['readdate', 'zyggl'],  # 查询并插入excel的内容
        batch=150  # 批量插入150行
    )
    # 导出多个文件
    ms.exportToExcel(**args)
