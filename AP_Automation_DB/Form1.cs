using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Data.SqlClient;

namespace AP_Automation_DB
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {

            string msg = string.Empty;

            db_util obj_db_util = new db_util();

            obj_db_util.system_name = AP_Automation_DB.Properties.Resources.system_name;
            obj_db_util.domain_name = AP_Automation_DB.Properties.Resources.domain_name;
            obj_db_util.source_code = AP_Automation_DB.Properties.Resources.source_code;
            obj_db_util.target_schema = AP_Automation_DB.Properties.Resources.target_schema;

            MessageBox.Show(obj_db_util.create_tables());
        }

        
        
    }
}
