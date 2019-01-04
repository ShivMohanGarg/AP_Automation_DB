using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
    

namespace AP_Automation_DB
{
    class db_util
    {

        private string _system_name;
        private string _domain_name;
        private string _source_code;
        private string _target_schema;

        public string system_name
        {
            get => _system_name;
            set => _system_name = AP_Automation_DB.Properties.Resources.system_name;
        }
        public string domain_name
        {
            get => _domain_name;
            set => _domain_name = AP_Automation_DB.Properties.Resources.domain_name;
        }

        public string source_code
        {
            get => _source_code;
            set => _source_code = AP_Automation_DB.Properties.Resources.source_code;
        }

        public string target_schema
        {
            get => _target_schema;
            set => _target_schema = AP_Automation_DB.Properties.Resources.target_schema;
        }

        private DataTable get_EntityMetadata()  
        {
            string strcommand = string.Empty;

            SqlConnection conn = new SqlConnection(AP_Automation_DB.Properties.Resources.metadata_connstr);

            strcommand = @"Select e.entity_key,s.source_name, e.entity_name,d.domain_name,  a.attribute_name,a.not_nullable_ind,a.ap_functional_ind,a.unique_index_ind,
                            a.column_index,adt.data_type_name,adt.precision_value,adt.scale_value,plt.platform_name ,se.source_entity_key
                            from 
                            [aut].[source_entity] se 
                            inner join [aut].[source] s on se.source_key =s.source_key
                            inner join [aut].[entity] e on se.entity_key = e.entity_key
                            inner join [aut].[domain] d on d.domain_key = e.domain_key
                            inner join [aut].[source_attribute] sa on se.source_entity_key = sa.source_entity_key
                            inner join [aut].[attribute] a on sa.attribute_key  =a.attribute_key
                            inner join [aut].[attribute_class] ac on a.attribute_class_key =ac.attribute_class_key
                            inner join [aut].[attribute_data_type] adt on ac.attribute_class_key = adt.attribute_class_key
                            inner join [aut].[platform] plt on adt.platform_key = plt.platform_key
                            where e.use_in_automation_ind=1 and s.source_code='" + _source_code +
                                                    @"'and d.domain_name = '" + _domain_name +
                                                    @"' and plt.platform_name= '" + _system_name + "' ";

            SqlCommand comm = new SqlCommand(strcommand, conn);

            try
            {
                conn.Open();
                SqlDataReader sdr = comm.ExecuteReader();
                DataTable dt = new DataTable();
                dt.Load(sdr);

                return dt;
            }
            catch (Exception ex)
            {

                throw ex;
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                    conn.Close();
            }
            
        }

        private DataTable get_DistinctEntities(ref DataTable dt)
        {
            DataView dv_metadata = new DataView(dt);
            DataTable distinct_entities = new DataTable();
            distinct_entities = dv_metadata.ToTable(true, "entity_name","entity_key");
            return distinct_entities;
        }

        private DataView get_EntityMetadata(ref DataTable dt_Metadata, string tablename )
        {
            DataView dv_table_metadata = new DataView(dt_Metadata);

            for (int i = 0; i < dt_Metadata.Rows.Count; i++)
            {
                // MessageBox.Show(distinct_entities.Rows[i][0].ToString());
                dv_table_metadata.RowFilter = "entity_name='" + tablename + "'";
            }
            return dv_table_metadata;
        }

        private DataView get_EntityUniqueMetadata(ref DataTable dt_Metadata, string tablename)
        {
            DataView dv_table_metadata = new DataView(dt_Metadata);

            for (int i = 0; i < dt_Metadata.Rows.Count; i++)
            {
                // MessageBox.Show(distinct_entities.Rows[i][0].ToString());
                dv_table_metadata.RowFilter = "entity_name='" + tablename + "' AND unique_index_ind=1";
            }
            return dv_table_metadata;
        }

        private void add_EntityReferenceMetadata(ref StringBuilder strb_table_script, ref StringBuilder strb_table_reference_script, string entity_key,string entity_name)
        {
            string strcommand = string.Empty;

            SqlConnection conn = new SqlConnection(AP_Automation_DB.Properties.Resources.metadata_connstr);

            strcommand = @"Select distinct er.parent_entity_key,
                er.relationship_key,
                ce.entity_key child_entity_key,
                ce.entity_name child_entity_name,
                pe.entity_key parent_entity_key,
                pe.entity_name parent_entity_name
                from [aut].[entity_relationship] er
                left join[aut].[entity] pe on er.parent_entity_key = pe.entity_key
                left join[aut].[relationship] r on er.relationship_key=r.relationship_key
                left join[aut].[entity] ce on er.child_entity_key = ce.entity_key
                where ce.entity_key = " + entity_key;

            SqlCommand comm = new SqlCommand(strcommand, conn);

            try
            {
                conn.Open();
                SqlDataReader sdr = comm.ExecuteReader();
                DataTable dt = new DataTable();
                dt.Load(sdr);

                //  return dt;
                string parent_entity_name = string.Empty;
                string attribute_name = string.Empty;
                string attribute_datatype = string.Empty;
                string attribute_not_nullable = string.Empty;
                string attribute_precision_value = string.Empty;
                string attribute_unique_index_ind = string.Empty;

                /* Reference code for add forieng key constraint
                ALTER TABLE [aut].[entity_relationship] WITH CHECK ADD CONSTRAINT[FK_entity_relationship_entity1] FOREIGN KEY([child_entity_key])
                REFERENCES[aut].[entity]([entity_key])
                GO
                ALTER TABLE [aut].[entity_relationship] CHECK CONSTRAINT [FK_entity_relationship_relationship]
                GO
                */
                strb_table_reference_script.Append(Environment.NewLine + Environment.NewLine);

                /* the code segment builds the definition of columns for the tables */
                for (int j = 0; j < dt.Rows.Count; j++)
                {
                    parent_entity_name = _domain_name + "_" + dt.Rows[j]["parent_entity_name"].ToString().Replace(" ", "_");
                    attribute_name = parent_entity_name + "_FK";
                    attribute_datatype = "VARCHAR";
                    /*TODO: Check if */
                    attribute_not_nullable = "NOT NULL";
                    attribute_precision_value = "(200)";
                    
                    strb_table_script.Append(Environment.NewLine + "\t,[" + attribute_name + "]" + " " + attribute_datatype + attribute_precision_value + " " + attribute_not_nullable);

                    strb_table_reference_script.Append(Environment.NewLine + "ALTER TABLE " + "[" + _target_schema + "].["+ entity_name + "] WITH CHECK ADD CONSTRAINT[FK_" + entity_name + "_" + parent_entity_name + "] ");
                    strb_table_reference_script.Append("FOREIGN KEY([" + attribute_name + "])");
                    strb_table_reference_script.Append(Environment.NewLine);
                    strb_table_reference_script.Append("REFERENCES " + "["+ _target_schema + "].[" + parent_entity_name + "]([" + attribute_name +  "])");
                    strb_table_reference_script.Append(Environment.NewLine + "GO");

                    // Optional  - Check Constaint
                    //strb_table_reference_script.Append(Environment.NewLine + "ALTER TABLE " + "[" + _target_schema + "].[" + entity_name + "] CHECK CONSTRAINT [FK_" + entity_name + "_" + parent_entity_name + "] ");
                    //strb_table_reference_script.Append(Environment.NewLine + "GO");

                }/*Attributes loop*/

            }
            catch (Exception ex)
            {

                throw ex;
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                    conn.Close();
            }
        }

        /*The function is not used currently but will be used in view script creation */
        private DataTable get_EntityIdentifierMetadata(string entity_key)
        {
            string strcommand = string.Empty;

            SqlConnection conn = new SqlConnection(AP_Automation_DB.Properties.Resources.metadata_connstr);

            strcommand = @"Select sei.source_entity_key,e.entity_key,	sei.source_table_name,	sei.source_column_name,	sei.source_column_group	,sei.source_column_order,s.source_code,s.source_name
                            from  [aut].[source_entity_identifier] sei inner join [aut].[source_entity] se 
                            on sei.source_entity_key=se.source_entity_key  
                            inner join [aut].[entity] e on se.entity_key =e.entity_key
                            inner join [aut].[source] s on se.source_key =s.source_key
                            where s.source_code='" + _source_code +
                            @"'AND e.entity_key = " + entity_key;

            SqlCommand comm = new SqlCommand(strcommand, conn);

            try
            {
                conn.Open();
                SqlDataReader sdr = comm.ExecuteReader();
                DataTable dt = new DataTable();
                dt.Load(sdr);

                return dt;
            }
            catch (Exception ex)
            {

                throw ex;
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                    conn.Close();
            }
        }

        private void add_BoilerPlateMetadata(ref StringBuilder strb_table_script, string entity_key)
        {
            string strcommand = string.Empty;

            SqlConnection conn = new SqlConnection(AP_Automation_DB.Properties.Resources.metadata_connstr);

            strcommand = @"Select bpa.attribute_name, ac.attribute_class_name, adt.data_type_name, adt.precision_value, adt.scale_value, plt.platform_name
                            from [aut].[boiler_plate_entity] bpe inner join [aut].[boiler_plate_attribute] bpa
                            on bpe.boiler_plate_pattern_key= bpa.boiler_plate_pattern_key
                            inner join [aut].[attribute_class] ac on bpa.attribute_class_key = ac.attribute_class_key
                            inner join [aut].[attribute_data_type] adt on ac.attribute_class_key = adt.attribute_class_key
                            inner join [aut].[platform] plt on adt.platform_key = plt.platform_key
                            where plt.platform_name='" + _system_name + "'" +
                            @"AND bpe.entity_key = " + entity_key;

            SqlCommand comm = new SqlCommand(strcommand, conn);

            try
            {
                conn.Open();
                SqlDataReader sdr = comm.ExecuteReader();
                DataTable dt = new DataTable();
                dt.Load(sdr);

                //  return dt;

                string attribute_name = string.Empty;
                string attribute_datatype = string.Empty;
                string attribute_not_nullable = string.Empty;
                string attribute_precision_value = string.Empty;
                string attribute_unique_index_ind = string.Empty;

                /* the code segment builds the definition of columns for the tables */
                for (int j = 0; j < dt.Rows.Count; j++)
                {

                    attribute_name = dt.Rows[j]["attribute_name"].ToString().Replace(" ", "_");
                    attribute_datatype = dt.Rows[j]["data_type_name"].ToString();
                    attribute_not_nullable = "NOT NULL";
                    attribute_precision_value = (dt.Rows[j]["precision_value"].ToString().Trim() == "" ? "" : "(" + dt.Rows[j]["precision_value"].ToString() + ")");
                    
                    strb_table_script.Append(Environment.NewLine + "\t,[" + attribute_name + "]" + " " + attribute_datatype + attribute_precision_value + " " + attribute_not_nullable);
                    if (j + 1 < dt.Rows.Count)
                        strb_table_script.Append(Environment.NewLine);
                    
                }/*Attributes loop*/

            }
            catch (Exception ex)
            {

                throw ex;
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                    conn.Close();
            }
        }

        
        /* The function iterates through the metadata for the entity and creates the script for the table entity and executes the table creation script on DB server */
        /* Catch the success and failure of the table generation and let the table generation pass on to next table creation if one of them fails */
        /*-e can persists the table generation process to the database */
        public string create_tables()
        {
            string msg = string.Empty;
            StringBuilder strb_tables_script;
            StringBuilder strb_table_script;
            StringBuilder strb_table_primary_script;
            StringBuilder strb_table_reference_script;
            string final_script = string.Empty;
            string final_reference_script = string.Empty;

            strb_tables_script = new StringBuilder();

            try
            {
                DataTable dt = this.get_EntityMetadata();

                DataTable dt_distinct_entities = this.get_DistinctEntities(ref dt);

                DataView dv_entity_metadata;

                 DataView dv_entity_unique_metadata;

                String primary_key_name = string.Empty;

                strb_table_reference_script = new StringBuilder(string.Empty);

                /*iterate for each entity/table in the tables/entities list */
                for (int i = 0; i < dt_distinct_entities.Rows.Count; i++)
                {
                    strb_table_script = new StringBuilder(string.Empty);
                    strb_table_primary_script = new StringBuilder(string.Empty);
                    
                    dv_entity_metadata = this.get_EntityMetadata(ref dt, dt_distinct_entities.Rows[i]["entity_name"].ToString());
                    
                    /*-- to do code will create the script for tables and possibly views to and will persist into the db one by one*/
                    strb_table_script.Append("CREATE TABLE ["+ _target_schema + "].[" + _domain_name + "_" +  dt_distinct_entities.Rows[i]["entity_name"].ToString().Replace(" ","_") + "] (" + Environment.NewLine);

                    string attribute_name = string.Empty;
                    string attribute_datatype = string.Empty;
                    string attribute_not_nullable = string.Empty;
                    string attribute_precision_value = string.Empty;
                    string attribute_unique_index_ind = string.Empty;
                    
                    primary_key_name = "PK_" + _domain_name + "_" + dt_distinct_entities.Rows[i]["entity_name"].ToString() + "_";

                   // strb_table_script.Append("\t");

                    strb_table_script.Append("\t /* Primary Key Columns */" + Environment.NewLine);

                    /*TODO: The primary key data type should be picked up from the identifier class for the selected system*/
                    strb_table_script.Append("\t[" + dt_distinct_entities.Rows[i]["entity_name"].ToString().Replace(" ", "_") + "_PK" + "] VARCHAR(200) NOT NULL" + Environment.NewLine);
                    
                    strb_table_primary_script.Append("\t[" + dt_distinct_entities.Rows[i]["entity_name"].ToString().Replace(" ", "_") + "_PK" + "] ASC");

                    /*TODO: The section should be configured as an optional based on flag from configuration to add the source columns into the target or not*/
                    strb_table_script.Append("\t /* Primary Key Source Columns */" + Environment.NewLine);

                    dv_entity_unique_metadata = this.get_EntityUniqueMetadata(ref dt, dt_distinct_entities.Rows[i]["entity_name"].ToString());

                     for (int k = 0; k < dv_entity_unique_metadata.Count; k++)
                    {
                        attribute_name = dv_entity_unique_metadata[k]["attribute_name"].ToString().Replace(" ", "_");
                        attribute_datatype = dv_entity_unique_metadata[k]["data_type_name"].ToString();
                        attribute_not_nullable = (dv_entity_unique_metadata[k]["not_nullable_ind"].ToString() == "True" ? "NOT NULL" : "");
                        attribute_precision_value = (dv_entity_unique_metadata[k]["precision_value"].ToString().Trim() == "" ? "" : "(" + dv_entity_unique_metadata[k]["precision_value"].ToString() + ")");
                        attribute_unique_index_ind = dv_entity_unique_metadata[k]["unique_index_ind"].ToString();

                        /* include only those columns that doesn't have unique index as they will be replaced with source entity identifier column(s) */
                        if (attribute_unique_index_ind == "True")
                        {
                            strb_table_script.Append("\t,[" + attribute_name + "]" + " " + attribute_datatype + attribute_precision_value + " " + attribute_not_nullable);
                            if (k + 1 < dv_entity_metadata.Count)
                                strb_table_script.Append(Environment.NewLine);
                        }
                    }


                    /* The code appends the reference columns  */
                    strb_table_script.Append("\t /* Reference Column(s) */");
                    this.add_EntityReferenceMetadata(ref strb_table_script,ref strb_table_reference_script, dt_distinct_entities.Rows[i]["entity_key"].ToString(),  _domain_name + "_" + dt_distinct_entities.Rows[i]["entity_name"].ToString().Replace(" ", "_") );

                    strb_table_script.Append(Environment.NewLine);

                    strb_table_script.Append("\t /* Other Columns */" + Environment.NewLine);

                    /* the code segment builds the definition of columns for the tables */
                    for (int j = 0; j < dv_entity_metadata.Count; j++)
                    {

                        attribute_name = dv_entity_metadata[j]["attribute_name"].ToString().Replace(" ", "_");
                        attribute_datatype = dv_entity_metadata[j]["data_type_name"].ToString();
                        attribute_not_nullable = (dv_entity_metadata[j]["not_nullable_ind"].ToString()=="True"? "NOT NULL":"");
                        attribute_precision_value = (dv_entity_metadata[j]["precision_value"].ToString().Trim()==""?"":"(" + dv_entity_metadata[j]["precision_value"].ToString() + ")");
                        attribute_unique_index_ind = dv_entity_metadata[j]["unique_index_ind"].ToString();

                        /* include only those columns that doesn't have unique index as they will be replaced with source entity identifier column(s) */
                        if (attribute_unique_index_ind == "False")
                        {
                            strb_table_script.Append("\t,[" + attribute_name + "]" + " " + attribute_datatype + attribute_precision_value + " " + attribute_not_nullable);
                            if (j + 1 < dv_entity_metadata.Count)
                                strb_table_script.Append(Environment.NewLine);
                        }
                    }/*Attributes loop*/


                    /* The code appends the boiler plate columns  */
                    strb_table_script.Append(Environment.NewLine + "\t /* Boiler Plate Column(s) */");
                    this.add_BoilerPlateMetadata(ref strb_table_script, dt_distinct_entities.Rows[i]["entity_key"].ToString());

                    /*This section appends the primary key contraint to the table script*/
                    strb_table_script.Append(Environment.NewLine + ",CONSTRAINT[" + primary_key_name + "] PRIMARY KEY CLUSTERED");
                    strb_table_script.Append(Environment.NewLine + "(" + Environment.NewLine + strb_table_primary_script + Environment.NewLine + ") WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]");
                    strb_table_script.Append(Environment.NewLine + ") ON [PRIMARY]");

                    //strb_table_script.Append(Environment.NewLine);
                    //strb_table_script.Append(strb_table_reference_script);

                    strb_tables_script.Append(strb_table_script.ToString());
                    strb_tables_script.Append(Environment.NewLine + Environment.NewLine);
                    strb_tables_script.Append("GO;" + Environment.NewLine + Environment.NewLine);

                }
                /*entities loop*/
                final_script = strb_tables_script.ToString() + Environment.NewLine + Environment.NewLine + strb_table_reference_script.ToString();
                //final_reference_script = strb_table_reference_script.ToString();
                return "Success";
            }
            catch (Exception ex)
            {
               throw ex;
            }
            finally
            {
               //
            }
        }
    }
}



// unused - outdated code 
//DataTable dt_entity_primary_metadata;
/*-- create the script for tables and possibly views to and will persist into the db one by one*/
//dt_entity_primary_metadata = this.get_EntityPrimaryMetadata(dt_distinct_entities.Rows[i]["entity_key"].ToString());
//for (int l = 0; l < dt_entity_primary_metadata.Rows.Count; l++)
//{
//    attribute_name = dt_entity_primary_metadata.Rows[l]["source_column_name"].ToString() + "_PK";
//    strb_table_primary_script.Append("\t[" + attribute_name + "] ASC");
//    primary_key_name = primary_key_name + attribute_name;
//    /* TODO - Currently the primary key data type is not derived from the system_name*/
//    strb_table_script.Append("[" + attribute_name + "] VARCHAR(200) NOT NULL" + Environment.NewLine);
//    if (l + 1 < dt_entity_primary_metadata.Rows.Count)
//    {
//        strb_table_script.Append("\t,");
//        strb_table_primary_script.Append("," + Environment.NewLine);
//        primary_key_name = primary_key_name + "_";
//    }
//}

