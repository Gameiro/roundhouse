using Oracle.ManagedDataAccess.Client;
using roundhouse.connections;
using roundhouse.databases;
using roundhouse.infrastructure.app;
using roundhouse.infrastructure.extensions;
using roundhouse.infrastructure.logging;
using roundhouse.parameters;
using System;
using System.Collections.Generic;
using System.Data;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

namespace roundhouse.databases.oracle
{
    public sealed class OracleDatabase : AdoNetDatabase
    {
        private string connect_options = "Integrated Security";

        public override bool split_batch_statements
        {
            get
            {
                return false;
            }
        }

        public override string sql_statement_separator_regex_pattern
        {
            get
            {
                return "(?<KEEP1>^(?:.)*(?:-{2}).*$)|(?<KEEP1>/{1}\\*{1}[\\S\\s]*?\\*{1}/{1})|(?<KEEP1>^|\\s)(?<BATCHSPLITTER>;)(?<KEEP2>\\s|$)";
            }
        }

        public override bool supports_ddl_transactions
        {
            get
            {
                return false;
            }
        }

        public OracleDatabase()
        {
        }

        private static string build_connection_string(string database_name, string connection_options)
        {
            return string.Format("Data Source={0};{1}", database_name, connection_options);
        }

        protected override void connection_specific_setup(IDbConnection connection)
        {
            ((OracleConnection)connection).InfoMessage += new OracleInfoMessageEventHandler((object sender, OracleInfoMessageEventArgs e) => Log.bound_to(this).log_a_debug_event_containing("  [SQL PRINT]: {0}{1}", new object[] { Environment.NewLine, e.Message }));
        }

        public override string create_database_script()
        {
            return string.Format("\r\n                DECLARE\r\n                    v_exists Integer := 0;\r\n                BEGIN\r\n                    SELECT COUNT(*) INTO v_exists FROM dba_users WHERE username = '{0}';\r\n                    IF v_exists = 0 THEN\r\n                        EXECUTE IMMEDIATE 'CREATE USER {0} IDENTIFIED BY {0}';\r\n                        EXECUTE IMMEDIATE 'GRANT CREATE SESSION TO {0}';\r\n                        EXECUTE IMMEDIATE 'GRANT RESOURCE TO {0}';                            \r\n                    END IF;\r\n                END;                        \r\n                ", base.database_name.to_upper());
        }

        private IParameter<IDbDataParameter> create_parameter(string name, DbType type, object value, int? size)
        {
            IDbCommand dbCommand = this.server_connection.underlying_type().CreateCommand();
            IDbDataParameter dbDataParameter = dbCommand.CreateParameter();
            dbCommand.Dispose();
            dbDataParameter.Direction = ParameterDirection.Input;
            dbDataParameter.ParameterName = name;
            dbDataParameter.DbType = type;
            IDbDataParameter dbDataParameter1 = dbDataParameter;
            object obj = value;
            if (obj == null)
            {
                obj = DBNull.Value;
            }
            dbDataParameter1.Value = obj;
            if (size.HasValue)
            {
                dbDataParameter.Size = size.Value;
            }
            return new AdoNetParameter(dbDataParameter);
        }

        public string create_sequence_script(string table_name)
        {
            return string.Format("\r\n                    DECLARE\r\n                        sequenceExists Integer := 0;\r\n                    BEGIN\r\n                        SELECT COUNT(*) INTO sequenceExists FROM user_objects WHERE object_type = 'SEQUENCE' AND UPPER(object_name) = UPPER('{0}_{1}ID');\r\n                        IF sequenceExists = 0 THEN   \r\n                        \r\n                            EXECUTE IMMEDIATE 'CREATE SEQUENCE {0}_{1}id\r\n                            START WITH 1\r\n                            INCREMENT BY 1\r\n                            MINVALUE 1\r\n                            MAXVALUE 999999999999999999999999999\r\n                            CACHE 20\r\n                            NOCYCLE \r\n                            NOORDER';\r\n                            \r\n                        END IF;\r\n                    END;\r\n              ", base.roundhouse_schema_name, table_name);
        }

        public override string delete_database_script()
        {
            return string.Format(" \r\n                DECLARE\r\n                    v_exists Integer := 0;\r\n                BEGIN\r\n                    SELECT COUNT(*) INTO v_exists FROM dba_users WHERE username = '{0}';\r\n                    IF v_exists > 0 THEN\r\n                        EXECUTE IMMEDIATE 'DROP USER {0} CASCADE';\r\n                    END IF;\r\n                END;", base.database_name.to_upper());
        }

        public string get_version_id_script()
        {
            return string.Format("\r\n                    SELECT id\r\n                    FROM (SELECT * FROM {0}_{1}\r\n                            WHERE \r\n                                NVL(repository_path, '') = NVL(:repository_path, '')\r\n                            ORDER BY entry_date DESC)\r\n                    WHERE ROWNUM < 2\r\n                ", base.roundhouse_schema_name, base.version_table_name);
        }

        public override void initialize_connections(ConfigurationPropertyHolder configuration_property_holder)
        {
            if (!string.IsNullOrEmpty(base.connection_string))
            {
                string[] strArrays = base.connection_string.Split(new char[] { ';' });
                string[] strArrays1 = strArrays;
                for (int i = 0; i < (int)strArrays1.Length; i++)
                {
                    string str = strArrays1[i];
                    if (string.IsNullOrEmpty(base.server_name) && str.to_lower().Contains("data source"))
                    {
                        base.database_name = str.Substring(str.IndexOf("=") + 1);
                    }
                }
                if (!base.connection_string.to_lower().Contains(this.connect_options.to_lower()))
                {
                    this.connect_options = string.Empty;
                    string[] strArrays2 = strArrays;
                    for (int j = 0; j < (int)strArrays2.Length; j++)
                    {
                        string str1 = strArrays2[j];
                        if (!str1.to_lower().Contains("data source"))
                        {
                            OracleDatabase oracleDatabase = this;
                            oracleDatabase.connect_options = string.Concat(oracleDatabase.connect_options, str1, ";");
                        }
                    }
                }
            }
            if (this.connect_options == "Integrated Security")
            {
                this.connect_options = "Integrated Security=yes;";
            }
            if (string.IsNullOrEmpty(base.connection_string))
            {
                base.connection_string = OracleDatabase.build_connection_string(base.database_name, this.connect_options);
            }
            configuration_property_holder.ConnectionString = base.connection_string;
            this.set_provider();
            if (string.IsNullOrEmpty(base.admin_connection_string))
            {
                base.admin_connection_string = Regex.Replace(base.connection_string, "Integrated Security=.*?;", "Integrated Security=yes;");
                base.admin_connection_string = Regex.Replace(base.admin_connection_string, "User Id=.*?;", string.Empty);
                base.admin_connection_string = Regex.Replace(base.admin_connection_string, "Password=.*?;", string.Empty);
            }
            configuration_property_holder.ConnectionStringAdmin = base.admin_connection_string;
        }

        public override long insert_version_and_get_version_id(string repository_path, string repository_version)
        {
            List<IParameter<IDbDataParameter>> parameters = new List<IParameter<IDbDataParameter>>()
            {
                this.create_parameter("repository_path", DbType.AnsiString, repository_path, new int?(255)),
                this.create_parameter("repository_version", DbType.AnsiString, repository_version, new int?(35)),
                this.create_parameter("user_name", DbType.AnsiString, base.user_name, new int?(50))
            };
            this.run_sql(this.insert_version_script(), ConnectionType.Default, parameters);
            List<IParameter<IDbDataParameter>> parameters1 = new List<IParameter<IDbDataParameter>>()
            {
                this.create_parameter("repository_path", DbType.AnsiString, repository_path, new int?(255))
            };
            List<IParameter<IDbDataParameter>> parameters2 = parameters1;
            return Convert.ToInt64(this.run_sql_scalar(this.get_version_id_script(), ConnectionType.Default, parameters2));
        }

        public string insert_version_script()
        {
            return string.Format("\r\n                    INSERT INTO {0}_{1}\r\n                    (\r\n                        id\r\n                        ,repository_path\r\n                        ,version\r\n                        ,entered_by\r\n                    )\r\n                    VALUES\r\n                    (\r\n                        {0}_{1}id.NEXTVAL\r\n                        ,:repository_path\r\n                        ,:repository_version\r\n                        ,:user_name\r\n                    )\r\n                ", base.roundhouse_schema_name, base.version_table_name);
        }

        public override string restore_database_script(string restore_from_path, string custom_restore_options)
        {
            return string.Empty;
        }

        public override void run_database_specific_tasks()
        {
            Log.bound_to(this).log_an_info_event_containing("Creating a sequence for the '{0}' table.", new object[] { base.version_table_name });
            this.run_sql(this.create_sequence_script(base.version_table_name), ConnectionType.Default);
            Log.bound_to(this).log_an_info_event_containing("Creating a sequence for the '{0}' table.", new object[] { base.scripts_run_table_name });
            this.run_sql(this.create_sequence_script(base.scripts_run_table_name), ConnectionType.Default);
            Log.bound_to(this).log_an_info_event_containing("Creating a sequence for the '{0}' table.", new object[] { base.scripts_run_errors_table_name });
            this.run_sql(this.create_sequence_script(base.scripts_run_errors_table_name), ConnectionType.Default);
        }

        public override void run_sql(string sql_to_run, ConnectionType connection_type)
        {
            Log.bound_to(this).log_a_debug_event_containing("Replacing script text \r\n with \n to be compliant with Oracle.", new object[0]);
            base.run_sql(sql_to_run.Replace("\r\n", "\n"), connection_type);
        }

        protected override object run_sql_scalar(string sql_to_run, ConnectionType connection_type, IList<IParameter<IDbDataParameter>> parameters)
        {
            Log.bound_to(this).log_a_debug_event_containing("Replacing \r\n with \n to be compliant with Oracle.", new object[0]);
            sql_to_run = sql_to_run.Replace("\r\n", "\n");
            object obj = new object();
            if (string.IsNullOrEmpty(sql_to_run))
            {
                return obj;
            }
            using (IDbCommand dbCommand = base.setup_database_command(sql_to_run, connection_type, parameters))
            {
                obj = dbCommand.ExecuteScalar();
                dbCommand.Dispose();
            }
            return obj;
        }

        public override void set_provider()
        {
            base.provider = "Oracle.ManagedDataAccess.Client";
        }

        public override string set_recovery_mode_script(bool simple)
        {
            return string.Empty;
        }
    }
}