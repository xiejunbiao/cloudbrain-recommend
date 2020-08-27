ps aux|grep built_offline_table_for_Rec.py|grep -v grep|cut -c 9-15|xargs kill -9
ps aux|grep update_offline_table_for_Rec.py|grep -v grep|cut -c 9-15|xargs kill -9
ps aux|grep rechot_mul_execu_post.py|grep -v grep|cut -c 9-15|xargs kill -9
