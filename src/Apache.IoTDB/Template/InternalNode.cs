using System;
using System.Collections.Generic;
using Thrift;

namespace Apache.IoTDB
{
    public class InternalNode : TemplateNode
    {
        private Dictionary<string, TemplateNode> children;
        private bool shareTime;
        public InternalNode(string name, bool shareTime) : base(name)
        {
            this.children = new Dictionary<string, TemplateNode>();
            this.shareTime = shareTime;
        }
        public override void addChild(TemplateNode node)
        {
            if (this.children.ContainsKey(node.Name))
            {
                throw new Exception("Duplicated child of node in template.");
            }
            this.children.Add(node.Name, node);
        }

        public override void deleteChild(TemplateNode node)
        {
            if (this.children.ContainsKey(node.Name))
            {
                this.children.Remove(node.Name);
            }
        }

        public override Dictionary<string, TemplateNode> getChildren()
        {
            return this.children;
        }
        public override bool isShareTime()
        {
            return this.shareTime;
        }
    }
}