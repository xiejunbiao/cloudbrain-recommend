from hotsale_offline_com.utils.dataBaseHandle import readMysqlmultiPd

def filter_spu_codes(ownerCode,cateCode):

    filt_mysqlCmd = "SELECT spu_code_list FROM owner_spus_for_hotsale\
     where owner_code='%s' and first_cate='%s'" % (ownerCode, cateCode)
    dataPd = readMysqlmultiPd(filt_mysqlCmd,'our_database')
    spu_code_list_str = dataPd['spu_code_list'][0]
    filt_mysqlCmd = "SELECT * FROM goods_spu_for_filter where goods_status =1 and store_status=1 and spu_code in (" + spu_code_list_str + ")"
    dataPd1 = readMysqlmultiPd(filt_mysqlCmd,'our_database')
    return dataPd1

if __name__ == '__main__':
    ownerCode = '*2018101111102593860'
    cateCode = '100001'
    data = filter_spu_codes(ownerCode,cateCode)
    print(data['spu_name'])