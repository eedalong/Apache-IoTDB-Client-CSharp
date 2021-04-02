namespace iotdb_client_csharp.client.utils
{
    public class Field
    {
        // TBD By Zengz
        private bool bool_val;
        private int int_val;
        private long long_val;
        private float float_val;
        private double double_val;
        private byte binary_val;

        private TSDataType data_type;
        
        public Field(TSDataType data_type){
            this.data_type = data_type;
        }
        public TSDataType get_data_type(){
            return this.data_type;
        }
        public void set_bool_value(bool value){
            bool_val = value;
        }
        public bool get_bool_value(){
            return bool_val;

        }
        public void set_int_value(int value){
            int_val = value;
        }
        public int get_int_value(){
            return int_val;
        }

    }
}