import dbe
import ctypes
import inspect

ctypes._dlopen('/localdisk/gal/root/igalink/omniscidb/build/Wrapper/libDBEngine.so', ctypes.RTLD_GLOBAL)

obj = dbe.PyDbEngine('/localdisk/gal/root/1/omniscidb/build/bin/data')
obj.executeDML("SELECT count(id) FROM omnisci_states where abbr = 'CA'")
obj.executeDML("SELECT id FROM omnisci_states LIMIT 10")
#obj.executeDML("SELECT id, abbr, name FROM omnisci_states LIMIT 10")