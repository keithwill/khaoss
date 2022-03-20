using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS
{
    public enum TransactionResult
    {
        None,
        Complete,
        FailedConcurrencyCheck,
        UnexpectedError
    }
}
