using System;
using System.Collections.Generic;
using System.Collections.Concurrent.Partitioners;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Types;

using OSGeo.GDAL;
using OSGeo.OGR;
using OSGeo.OSR;


namespace osm2mssql
{
    class Program
    {
        static TraceSwitch TRACE_SWITCH;

        static void Main(string[] args)
        {
            // Set global exception handler
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);

            // Enable Tracing
            TRACE_SWITCH = new TraceSwitch("TraceSwitch", "Default trace switch", "3");

            // Get SQL Server configs
            string MSSQLConnectionString = ConfigurationManager.ConnectionStrings["MSSQLConnectionString"].ConnectionString;
            int SQLBatchSize = int.Parse(ConfigurationManager.AppSettings["SQLBatchSize"]);
            int SQLThreads = int.Parse(ConfigurationManager.AppSettings["SQLThreads"]);

            // Configure SQL Server spatial types
            SqlServerTypes.Utilities.LoadNativeAssemblies(AppDomain.CurrentDomain.BaseDirectory);

            // Configure OGR
            GdalConfiguration.ConfigureOgr();
            Gdal.SetConfigOption("OGR_INTERLEAVED_READING", "YES");
            Gdal.SetConfigOption("OSM_COMPRESS_NODES", "YES");
            Gdal.SetConfigOption("CPL_TMPDIR", ConfigurationManager.AppSettings["OSMTmpPath"]);
            Gdal.SetConfigOption("OSM_MAX_TMPFILE_SIZE", ConfigurationManager.AppSettings["OSMTmpFileSize"]);
            Gdal.SetConfigOption("OSM_CONFIG_FILE", ConfigurationManager.AppSettings["OSMConfigFile"]);
            DataSource OGRDataSource = Ogr.Open(ConfigurationManager.AppSettings["OSMFile"], 0);

            // Drop SQL tables 
            DropTables(OGRDataSource, MSSQLConnectionString);

            // Create SQL tables and return ADO.NET DataSet of with DataTables
            // DataTables will be used buffer records before SQL bulk insert
            DataSet OSMDataSet = CreateTables(OGRDataSource, MSSQLConnectionString);

            // Start Timer
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            // Do work son!
            log(TraceLevel.Info, "Begin Processing...");
            DoWork(OGRDataSource, OSMDataSet, MSSQLConnectionString, SQLThreads, SQLBatchSize);

            // Create Indexes
            CreateIndexes(OGRDataSource, MSSQLConnectionString);

            // Stop Timer 
            stopwatch.Stop();
            log(TraceLevel.Info, string.Format("Time elapsed: {0}", stopwatch.Elapsed));

        }

        static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            log(TraceLevel.Error, ((Exception)e.ExceptionObject).Message);
            Environment.Exit(1);
        }

        static void log(TraceLevel level, string message)
        {
            bool log = false;
            switch (level)
            {
                case TraceLevel.Info:
                    log = TRACE_SWITCH.TraceInfo;
                    break;
                case TraceLevel.Error:
                    log = TRACE_SWITCH.TraceError;
                    break;
                case TraceLevel.Verbose:
                    log = TRACE_SWITCH.TraceVerbose;
                    break;
                case TraceLevel.Warning:
                    log = TRACE_SWITCH.TraceWarning;
                    break;
            }
            Trace.WriteLineIf(log, string.Format("{0} {1}: {2}", DateTime.Now.ToString("yyyyMMdd HH:mm:ss.ff"), level.ToString(), message));
        }

        static void DropTables(DataSource OGRDataSource, string MSSQLConnectionString)
        {
            for (int iLayer = 0; iLayer < OGRDataSource.GetLayerCount(); iLayer++)
            {
                Layer OGRLayer = OGRDataSource.GetLayerByIndex(iLayer);
                string layerName = OGRLayer.GetName();

                using (SqlConnection con = new SqlConnection(MSSQLConnectionString))
                {
                    con.Open();
                    try
                    {
                        // Check if table exists
                        bool tableExists;
                        using (SqlCommand checkCmd = new SqlCommand(string.Format("SELECT OBJECT_ID('{0}');", layerName), con))
                        {
                            tableExists = (!(checkCmd.ExecuteScalar() is DBNull));
                        }
                        if (tableExists)
                        {
                            using (SqlCommand dropCmd = new SqlCommand(string.Format("DROP TABLE [{0}];", layerName), con))
                            {
                                log(TraceLevel.Info, string.Format("Dropping table {0}...", layerName));
                                dropCmd.ExecuteNonQuery();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        log(TraceLevel.Error, e.Message);
                        Environment.Exit(1);
                    }
                }
            }
        }

        static DataSet CreateTables(DataSource OGRDataSource, string MSSQLConnectionString)
        {
            // Create OSM ADO.NET DataSet 
            DataSet OSMDataSet = new DataSet();

            // Create SQL Tables
            for (int iLayer = 0; iLayer < OGRDataSource.GetLayerCount(); iLayer++)
            {
                Layer OGRLayer = OGRDataSource.GetLayerByIndex(iLayer);
                string layerName = OGRLayer.GetName();

                // Construct CREATE TABLE statement
                StringBuilder SQLCreateCmd = new StringBuilder();
                SQLCreateCmd.Append(string.Format("CREATE TABLE [{0}] (", layerName));
                SQLCreateCmd.Append("[id] [INT] IDENTITY(1,1), ");
                FeatureDefn OGRLayerDef = OGRLayer.GetLayerDefn();
                for (int iField = 0; iField < OGRLayerDef.GetFieldCount(); iField++)
                {
                    FieldDefn OGRFieldDef = OGRLayerDef.GetFieldDefn(iField);
                    SQLCreateCmd.Append(string.Format("[{0}] " + "[VARCHAR](MAX), ", OGRFieldDef.GetName()));
                }
                SQLCreateCmd.Append("[ogr_geometry] [GEOMETRY]);");

                // Construct SQL statement used to get table schema
                // We use this to set a DataTable object schema after we create the SQL table
                string SQLSchemaCmd = string.Format("SET FMTONLY ON; SELECT * FROM [{0}]; SET FMTONLY OFF;", layerName);

                // Execute SQL 
                DataTable table = new DataTable();
                using (SqlConnection con = new SqlConnection(MSSQLConnectionString))
                {
                    con.Open();
                    try
                    {
                        // Create SQL table
                        using (SqlCommand cmd = new SqlCommand(SQLCreateCmd.ToString(), con))
                        {
                            log(TraceLevel.Info, string.Format("Creating table {0}...", layerName));
                            cmd.ExecuteNonQuery();
                        }

                        // Set DataTable object schema
                        using (SqlDataAdapter da = new SqlDataAdapter(SQLSchemaCmd, con))
                        {
                            da.FillSchema(table, SchemaType.Source);

                            // Hack to set ogr_geometry datatable column to correct SQLGeometry type
                            // http://msdn.microsoft.com/en-us/library/ms143179(v=sql.110).aspx#Y2686
                            // http://connect.microsoft.com/SQLServer/feedback/details/685654/invalidcastexception-retrieving-sqlgeography-column-in-ado-net-data-reader
                            table.Columns["ogr_geometry"].DataType = typeof(SqlGeometry);

                            // Add DataTable to OSM DataSet
                            OSMDataSet.Tables.Add(table);
                        }
                    }
                    catch (Exception e)
                    {
                        log(TraceLevel.Error, e.Message);
                        Environment.Exit(1);
                    }
                }
            }
            return OSMDataSet;
        }

        static void DoWork(DataSource OGRDataSource, DataSet OSMDataSet, string MSSQLConnectionString, int SQLThreads, int SQLBatchSize)
        {
            ChunkPartitioner<DataTable> bufferEnumerator = new ChunkPartitioner<DataTable>(ReadData(OGRDataSource, OSMDataSet, SQLBatchSize), 1);
            ParallelOptions pOptions = new ParallelOptions();
            pOptions.MaxDegreeOfParallelism = SQLThreads;

            try
            {
                Parallel.ForEach(bufferEnumerator, pOptions, buffer =>
                {
                    WriteBuffer(buffer, MSSQLConnectionString);
                });
            }
            catch (Exception e)
            {
                log(TraceLevel.Error, e.Message);
                Environment.Exit(1);
            }
        }

        static IEnumerable<DataTable> ReadData(DataSource OGRDataSource, DataSet OSMDataSet, int SQLBatchSize)
        {
            int featureCount = 0;

            // Create coordinate transformation
            SpatialReference sourceSRS = new SpatialReference("");
            sourceSRS.ImportFromEPSG(4326);
            SpatialReference targetSRS = new SpatialReference("");
            targetSRS.ImportFromEPSG(900913);
            CoordinateTransformation transform = new CoordinateTransformation(sourceSRS, targetSRS);

            // Use interleaved reading - http://www.gdal.org/drv_osm.html
            bool bHasLayersNonEmpty = false;
            do
            {
                bHasLayersNonEmpty = false;
                for (int iLayer = 0; iLayer < OGRDataSource.GetLayerCount(); iLayer++)
                {
                    Layer OGRLayer = OGRDataSource.GetLayerByIndex(iLayer);
                    log(TraceLevel.Verbose, string.Format("Processing {0}...", OGRLayer.GetName()));
                    FeatureDefn OGRFeatDef = OGRLayer.GetLayerDefn();
                    DataTable buffer = OSMDataSet.Tables[OGRLayer.GetName()];

                    Feature feat;
                    while ((feat = OGRLayer.GetNextFeature()) != null)
                    {
                        bHasLayersNonEmpty = true;

                        // Commit buffer larger than batch size
                        if (buffer.Rows.Count >= SQLBatchSize)
                        {
                            yield return buffer.Copy();
                            buffer.Rows.Clear();
                        }

                        // Fill buffer row
                        DataRow row = buffer.NewRow();
                        for (int iField = 0; iField < OGRFeatDef.GetFieldCount(); iField++)
                        {
                            if (feat.IsFieldSet(iField))
                            {
                                // Add one to skip id IDENTITY column
                                row[iField + 1] = feat.GetFieldAsString(iField);
                            }
                        }

                        // Get OGR geometry object
                        Geometry geom = feat.GetGeometryRef();

                        // Project from EPSG:4326 to EPSG:900913
                        geom.Transform(transform);

                        // Serialize to WKB 
                        byte[] geomBuffer = new byte[geom.WkbSize()];
                        geom.ExportToWkb(geomBuffer);

                        // Set ogr_geometry buffer column from WKB
                        try
                        {
                            row["ogr_geometry"] = SqlGeometry.STGeomFromWKB(new SqlBytes(geomBuffer), 900913);
                            // Add row to buffer
                            buffer.Rows.Add(row);
                        }
                        catch (Exception e)
                        {
                            log(TraceLevel.Warning, string.Format("Cannot process osm_id: {0} ({1})", feat.GetFID(), e.Message));
                        }

                        // Update progress
                        featureCount++;
                    }

                    // Commit buffer before moving on to another layer
                    if (buffer.Rows.Count != 0)
                    {
                        yield return buffer.Copy();
                        buffer.Rows.Clear();
                    }
                }
            } while (bHasLayersNonEmpty);
        }

        static void WriteBuffer(DataTable buffer, string MSSQLConnectionString)
        {
            using (SqlConnection con = new SqlConnection(MSSQLConnectionString))
            {
                con.Open();
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(con, SqlBulkCopyOptions.TableLock, null))
                {
                    bulkCopy.DestinationTableName = buffer.TableName;
                    bulkCopy.BulkCopyTimeout = 0;
                    log(TraceLevel.Verbose, string.Format("Writing {0} records to {1}", buffer.Rows.Count, buffer.TableName));
                    bulkCopy.WriteToServer(buffer);
                }
            }
        }

        static void CreateIndexes(DataSource OGRDataSource, string MSSQLConnectionString)
        {
            // Create coordinate transformation
            SpatialReference sourceSRS = new SpatialReference("");
            sourceSRS.ImportFromEPSG(4326);
            SpatialReference targetSRS = new SpatialReference("");
            targetSRS.ImportFromEPSG(900913);
            CoordinateTransformation transform = new CoordinateTransformation(sourceSRS, targetSRS);

            for (int iLayer = 0; iLayer < OGRDataSource.GetLayerCount(); iLayer++)
            {
                Layer OGRLayer = OGRDataSource.GetLayerByIndex(iLayer);
                string layerName = OGRLayer.GetName();

                // Get extent in EPSG:900913
                Envelope extent = new Envelope();
                OGRLayer.GetExtent(extent,0);
                double[] ll = new double[] { extent.MinX, extent.MinY };
                double[] ur = new double[] { extent.MaxX, extent.MaxY };
                transform.TransformPoint(ll);
                transform.TransformPoint(ur);

                using (SqlConnection con = new SqlConnection(MSSQLConnectionString))
                {
                    con.Open();
                    try
                    {
                        // Create primary key/clustered index
                        string pkSQL = string.Format("ALTER TABLE [{0}] ADD CONSTRAINT [PK_{0}] PRIMARY KEY CLUSTERED([id] ASC)  WITH (SORT_IN_TEMPDB = ON);", layerName);
                        using (SqlCommand pkCmd = new SqlCommand(pkSQL, con))
                        {
                            log(TraceLevel.Info, string.Format("Creating clustured index pk_{0} on table {0}...", layerName));
                            pkCmd.CommandTimeout = 0;
                            pkCmd.ExecuteNonQuery();
                        }

                        // Create spatial index
                        OGRLayer.GetExtent(extent, 0);
                        string sidxSQL = string.Format("CREATE SPATIAL INDEX sidx_{0}_ogr_geometry ON [{0}](ogr_geometry) WITH (BOUNDING_BOX = ( {1}, {2}, {3}, {4} ), SORT_IN_TEMPDB = ON );", layerName, ll[0], ll[1], ur[0], ur[1]);
                        using (SqlCommand sidxCmd = new SqlCommand(sidxSQL, con))
                        {
                            log(TraceLevel.Info, string.Format("Creating spatial index sidx_{0}_ogr_geometry on table {0}...", layerName));
                            sidxCmd.CommandTimeout = 0;
                            sidxCmd.ExecuteNonQuery();
                        }
                    }
                    catch (Exception e)
                    {
                        log(TraceLevel.Error, e.Message);
                        Environment.Exit(1);
                    }
                }
            }
        }
    }
}
