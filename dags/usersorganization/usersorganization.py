from airflow.hooks.base import BaseHook
from airflow.models import Variable
from coopdevsutils.coopdevsutils import executequery, querytodataframe

from hashlib import pbkdf2_hmac
import binascii
import random
import string
import paramiko
import pandas as pd


def change_users_organization():
    connsuper = BaseHook.get_connection('SUPERSET').get_hook().get_sqlalchemy_engine()
    letters = string.ascii_lowercase
    salt = bytes(''.join(random.choice(letters) for i in range(15)), 'utf-8')
    n = 1500000
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(Variable.get("sftp_host"), Variable.get("sftp_port"),
                Variable.get("sftp_user"), Variable.get("sftp_password"))
    remote_path = "/csv"
    sftp_client = ssh.open_sftp()
    files = sftp_client.listdir(remote_path)

    for filename in files:
        if filename=="superset_users.csv":
            with sftp_client.open(remote_path + "/" + filename, "r") as f:
                df = pd.read_csv(f, delimiter=";")
                for index, row in df.iterrows():
                    user = row['Username']
                    email = row['Email']
                    password = row['Password']

                    firstName = row['FirstName']
                    qry = (f"""
                        select count(*) as cnt 
                        from ab_user 
                        where email='{email}' 
                           or username='{user}'
                           """)
                    df1 = querytodataframe(qry, ['cnt'], connsuper)
                    # validar usuari no repetit
                    if df1.iloc[0]['cnt'] > 0:
                        print(f"Repeated user {user} or email {email}. Do nothing.")
                    else:
                        try:

                            executequery('BEGIN', connsuper)

                            ln = "."
                            fn = firstName


                            # crear rol
                            qry = f"insert into ab_role values (nextval('ab_role_id_seq'::regclass), 'rl_{user}');"
                            executequery(qry, connsuper)

                            # crear usuari
                            dk = pbkdf2_hmac('sha256', bytes(password, 'utf-8'), salt, n)
                            dk1 = binascii.hexlify(dk).decode('utf-8')
                            qry = f"""insert into ab_user values (nextval('ab_user_id_seq'::regclass),
                                 '{fn}', '{ln}', '{user}', 'xxxxxxxx',
                                 true, '{email}', null, 0, 0, current_timestamp, current_timestamp, 1,1 );
                                """
                            executequery(qry, connsuper)
                            qry = f"update ab_user set password='pbkdf2:sha256:{str(n)}${salt.decode('utf-8')}${dk1}' where username='{user}';"
                            executequery(qry, connsuper)

                            # assignar usuari a rol
                            qry = f"""insert into ab_user_role 
                                   select nextval('ab_user_role_id_seq'::regclass), au.id, a.id  
                                   from ab_role a, ab_user au 
                                   where a.name in ('Reader Coordinadora', 'rl_{user}') and au.username ='{user}';
                            """

                            executequery(qry, connsuper)

                            # row level security filter

                            clausein =f" coordinator_name in (''{fn}'')  "


                            # Crea filtre IN
                            qry = f"""
                                insert into row_level_security_filters values (now(), now(), 
                                    nextval('row_level_security_filters_id_seq'::regclass), 
                                    '{clausein}',1,1,'Regular',null, 'RLS_IN_{user}', null
                                    );
                            """
                            print(qry)
                            executequery(qry, connsuper)



                            # filter roles
                            qry = f"""insert into rls_filter_roles 
                                   select nextval('rls_filter_roles_id_seq'::regclass), ar.id, rlsf.id 
                                   from ab_role ar, row_level_security_filters rlsf 
                                   where ar.name = 'rl_{user}' and rlsf.name in ('RLS_IN_{user}')
                                """
                            print(qry)
                            executequery(qry, connsuper)

                            # filter tables
                            qry = f"""insert into rls_filter_tables 
                                  select nextval('rls_filter_tables_id_seq'::regclass), t.id, rlsf.id 
                                  from "tables" t, row_level_security_filters rlsf 
                                  where table_name in ('comunitats_obertes', 'geografia_comunitats', 'geografia_xinxetes', 'xinxetes' , 'xinxetes_ubicacio') 
                                      and rlsf.name='RLS_IN_{user}';
                                """
                            print(qry)
                            executequery(qry, connsuper)


                        except Exception as e:
                            print(e)
                            executequery('ROLLBACK', connsuper)
                            # raise ValueError("Error en alguna pas.")


                f.close()
            sftp_client.posix_rename(remote_path + "/" + filename,
                                     remote_path + "/" + filename.replace(".csv", ".loaded"))
