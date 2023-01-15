using System.Collections.Generic;
using System.Linq;

namespace Apache.IoTDB
{
    public class Utils
    {
        public bool IsSorted(IList<long> collection)
        {
            for (var i = 1; i < collection.Count; i++)
            {
                if (collection[i] < collection[i - 1])
                {
                    return false;
                }
            }

            return true;
        }

        public int VerifySuccess(TSStatus status, int successCode, int redirectRecommendCode)
        {
            if (status.__isset.subStatus)
            {
                if (status.SubStatus.Any(subStatus => VerifySuccess(subStatus, successCode, redirectRecommendCode) != 0))
                {
                    return -1;
                }

                return 0;
            }

            if (status.Code == successCode || status.Code == redirectRecommendCode)
            {
                return 0;
            }

            return -1;
        }
    }
}