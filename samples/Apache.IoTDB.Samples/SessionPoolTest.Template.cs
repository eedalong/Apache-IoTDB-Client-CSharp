using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.IoTDB.DataStructure;
namespace Apache.IoTDB.Samples
{
    public partial class SessionPoolTest
    {
        public async Task TestCreateAndDropSchemaTemplate()
        {
            var session_pool = new SessionPool(host, port, pool_size);
            await session_pool.Open(false);
            if (debug) session_pool.OpenDebugMode();

            System.Diagnostics.Debug.Assert(session_pool.IsOpen());
            var status = 0;
            await session_pool.DropSchemaTemplateAsync(test_template_name);

            MeasurementNode node1 = new MeasurementNode(test_measurements[1], TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY);
            MeasurementNode node2 = new MeasurementNode(test_measurements[2], TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY);
            MeasurementNode node3 = new MeasurementNode(test_measurements[3], TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY);
            MeasurementNode node4 = new MeasurementNode(test_measurements[4], TSDataType.FLOAT, TSEncoding.PLAIN, Compressor.SNAPPY);
            Console.WriteLine("node1 name : {0}", node1.Name);

            Template template = new Template(test_template_name);
            template.addToTemplate(node1);
            template.addToTemplate(node2);
            template.addToTemplate(node3);
            template.addToTemplate(node4);

            status = await session_pool.CreateSchemaTemplateAsync(template);
            System.Diagnostics.Debug.Assert(status == 0);
            var templates = await session_pool.ShowAllTemplatesAsync();
            foreach (var t in templates)
            {
                Console.WriteLine("template name :\t{0}", t);
            }
            status = await session_pool.DropSchemaTemplateAsync(test_template_name);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.DeleteStorageGroupAsync(test_group_name);
            await session_pool.Close();
            Console.WriteLine("TestCreateAndDropSchemaTemplate Passed!");
        }

        public async Task TestSetAndUnsetSchemaTemplate()
        {
            var session_pool = new SessionPool(host, port, pool_size);
            await session_pool.Open(false);
            if (debug) session_pool.OpenDebugMode();

            System.Diagnostics.Debug.Assert(session_pool.IsOpen());
            var status = 0;
            await session_pool.DeleteStorageGroupAsync(test_group_name);
            await session_pool.UnsetSchemaTemplateAsync(string.Format("{0}.{1}", test_group_name, test_device), "template");
            await session_pool.DropSchemaTemplateAsync(test_template_name);

            MeasurementNode node1 = new MeasurementNode(test_measurements[1], TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY);
            MeasurementNode node2 = new MeasurementNode(test_measurements[2], TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY);
            MeasurementNode node3 = new MeasurementNode(test_measurements[3], TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY);
            MeasurementNode node4 = new MeasurementNode(test_measurements[4], TSDataType.FLOAT, TSEncoding.PLAIN, Compressor.SNAPPY);

            Template template = new Template(test_template_name);
            template.addToTemplate(node1);
            template.addToTemplate(node2);
            template.addToTemplate(node3);
            template.addToTemplate(node4);

            status = await session_pool.CreateSchemaTemplateAsync(template);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.SetSchemaTemplateAsync(test_template_name, string.Format("{0}.{1}", test_group_name, test_device));
            System.Diagnostics.Debug.Assert(status == 0);
            var paths = await session_pool.ShowPathsTemplateSetOnAsync(test_template_name);
            foreach (var p in paths)
            {
                Console.WriteLine("path :\t{0}", p);
            }
            status = await session_pool.UnsetSchemaTemplateAsync(string.Format("{0}.{1}", test_group_name, test_device), test_template_name);
            status = await session_pool.DropSchemaTemplateAsync(test_template_name);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.DeleteStorageGroupAsync(test_group_name);
            await session_pool.Close();
            Console.WriteLine("TestSetAndUnsetSchemaTemplate Passed!");
        }
        public async Task TestAddAlignedMeasurements()
        {
            var session_pool = new SessionPool(host, port, pool_size);
            await session_pool.Open(false);
            if (debug) session_pool.OpenDebugMode();

            System.Diagnostics.Debug.Assert(session_pool.IsOpen());
            var status = 0;
            await session_pool.DeleteStorageGroupAsync(test_group_name);
            await session_pool.DropSchemaTemplateAsync(test_template_name);

            MeasurementNode node1 = new MeasurementNode(test_measurements[1], TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY);
            MeasurementNode node2 = new MeasurementNode(test_measurements[2], TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY);
            MeasurementNode node3 = new MeasurementNode(test_measurements[3], TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY);
            MeasurementNode node4 = new MeasurementNode(test_measurements[4], TSDataType.FLOAT, TSEncoding.PLAIN, Compressor.SNAPPY);
            var measurements = new List<MeasurementNode>() { node3, node4 };

            Template template = new Template(test_template_name, true);
            template.addToTemplate(node1);
            template.addToTemplate(node2);
            status = await session_pool.CreateSchemaTemplateAsync(template);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.AddAlignedMeasurementsInTemplateAsync(test_template_name, measurements);
            System.Diagnostics.Debug.Assert(status == 0);
            var measurements_get = await session_pool.ShowMeasurementsInTemplateAsync(test_template_name);
            foreach (var m in measurements_get)
            {
                Console.WriteLine("measurement :\t{0}", m);
            }
            // status = await session_pool.SetSchemaTemplateAsync(test_template_name, string.Format("{0}.{1}", test_group_name, test_device));
            // System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.DropSchemaTemplateAsync(test_template_name);
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.Close();
            Console.WriteLine("TestAddAlignedMeasurements Passed!");
        }
        public async Task TestAddUnalignedMeasurements()
        {
            var session_pool = new SessionPool(host, port, pool_size);
            await session_pool.Open(false);
            if (debug) session_pool.OpenDebugMode();

            System.Diagnostics.Debug.Assert(session_pool.IsOpen());
            var status = 0;
            await session_pool.DeleteStorageGroupAsync(test_group_name);
            await session_pool.DropSchemaTemplateAsync(test_template_name);

            MeasurementNode node1 = new MeasurementNode(test_measurements[1], TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY);
            MeasurementNode node2 = new MeasurementNode(test_measurements[2], TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY);
            MeasurementNode node3 = new MeasurementNode(test_measurements[3], TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY);
            MeasurementNode node4 = new MeasurementNode(test_measurements[4], TSDataType.FLOAT, TSEncoding.PLAIN, Compressor.SNAPPY);
            var measurements = new List<MeasurementNode>() { node1, node2, node3, node4 };

            Template template = new Template(test_template_name);
            status = await session_pool.CreateSchemaTemplateAsync(template);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.AddUnalignedMeasurementsInTemplateAsync(test_template_name, measurements);
            System.Diagnostics.Debug.Assert(status == 0);
            var measurements_get = await session_pool.ShowMeasurementsInTemplateAsync(test_template_name);
            foreach (var m in measurements_get)
            {
                Console.WriteLine("measurement :\t{0}", m);
            }
            // status = await session_pool.SetSchemaTemplateAsync(test_template_name, string.Format("{0}.{1}", test_group_name, test_device));
            // System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.DropSchemaTemplateAsync(test_template_name);
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.Close();
            Console.WriteLine("TestAddUnalignedMeasurements Passed!");
        }
    }

}