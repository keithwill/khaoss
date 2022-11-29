using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS
{
    public class OptimisticConcurrencyException : Exception
    {
        public OptimisticConcurrencyException(string key, int saveVersion, int actualVersion) : 
            base($"Could not save {key} with {saveVersion} as the current version is {actualVersion}")
        {
        }
    }
}
