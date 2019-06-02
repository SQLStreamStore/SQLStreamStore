#define _GNU_SOURCE
#include <dlfcn.h>
#include <fcntl.h>
typedef int (*orig_open_f_type)(const char *pathname, int flags);
int open(const char *pathname, int flags, ...) {
    static orig_open_f_type orig_open;
    if (!orig_open) {
           orig_open = (orig_open_f_type)dlsym(RTLD_NEXT, "open");
        }
        return orig_open(pathname, flags & ~O_DIRECT);
}
