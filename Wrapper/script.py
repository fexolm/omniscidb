import dbe
import ctypes
import inspect

ctypes._dlopen('/localdisk/gal/root/omniscidb/Wrapper/libDBEngine.so', ctypes.RTLD_GLOBAL)

obj = dbe.PyDbEngine('/localdisk/gal/root/1/omniscidb/build/bin/data')
obj.execute("SELECT count(id) FROM omnisci_states where abbr = 'CA'", 0)

