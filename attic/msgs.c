#include <stdint.h>

#include "firebird/impl/msg_helper.h"

extern void addMessage(int code, const char* message);

typedef unsigned short USHORT;
typedef USHORT ISC_USHORT;
typedef intptr_t ISC_STATUS;
typedef long SLONG;

#define stringify_literal(x) #x

#define FB_IMPL_MSG_NO_SYMBOL(facility, number, text)

#define FB_IMPL_MSG_SYMBOL(facility, number, symbol, text)

#define FB_IMPL_MSG(facility, number, symbol, sqlCode, sqlClass, sqlSubClass, text) \
    addMessage(make_isc_code(FB_IMPL_MSG_FACILITY_##facility, number), stringify_literal(text));

int make_isc_code(int facility, int code) {
    ISC_USHORT t1 = facility;
    t1 &= 0x1F;
    ISC_STATUS t2 = t1;
    t2 <<= 16;
    ISC_STATUS t3 = code;
    code &= 0x3FFF;
    return t2 | t3 | ((ISC_STATUS) 0x14000000);
}

void process_messages() {
    #include "firebird/impl/msg/all.h"
}