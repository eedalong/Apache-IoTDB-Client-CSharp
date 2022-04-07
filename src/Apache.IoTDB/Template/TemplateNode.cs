using System.Collections.Generic;
using System.IO;
using Apache.IoTDB.DataStructure;
namespace Apache.IoTDB
{
    public abstract class TemplateNode
    {
        private string name;
        public TemplateNode(string name)
        {
            this.name = name;
        }
        public string Name
        {
            get
            {
                return name;
            }
        }

        public virtual Dictionary<string, TemplateNode> getChildren()
        {
            return null;
        }

        public virtual void addChild(TemplateNode node) { }
        public virtual void deleteChild(TemplateNode node) { }
        public virtual bool isMeasurement()
        {
            return false;
        }
        public virtual bool isShareTime()
        {
            return false;
        }
        public virtual byte[] ToBytes()
        {
            return null;
        }
    }
}