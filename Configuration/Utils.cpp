#include "Utils.h"

#include <locale>
#include <codecvt>
#include <cassert>

using namespace std;

namespace Configuration
{
  string WcharToUTF8(const wstring& str)
  {
    return wstring_convert<codecvt_utf8<wchar_t>>().to_bytes(str);
  }

  wstring UTF8ToWchar(const string& str)
  {
    return wstring_convert<codecvt_utf8<wchar_t>>().from_bytes(str);
  }

  string WideToNarrowStr(const wstring& wide, char replacementChar)
  {
    string narrow;

    narrow.resize(wide.size());

    use_facet<ctype<wchar_t>>(locale()).narrow(wide.data(), wide.data() + wide.size(),
                                               replacementChar, &narrow[0]);

    assert(strlen(narrow.c_str()) <= narrow.size());

    narrow.resize(strlen(narrow.c_str()));

    return narrow;
  }

  wstring NarrowToWideStr(const string& narrow)
  {
    wstring wide;

    wide.resize(narrow.size());

    use_facet<ctype<wchar_t>>(locale()).widen(narrow.data(), narrow.data() + narrow.size(), &wide[0]);

    assert(wcslen(wide.c_str()) <= wide.size());

    wide.resize(wcslen(wide.c_str()));

    return wide;
  }
}
