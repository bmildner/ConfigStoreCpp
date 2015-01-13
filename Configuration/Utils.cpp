// Copyright (c) 2014 by Bertolt Mildner
// All rights reserved.

#include "Utils.h"

#include <locale>
#include <codecvt>
#include <cassert>

#include "RandomNumberGenerator.h"

#ifdef WIN32

# define WIN32_LEAN_AND_MEAN
# define NOMINMAX
# include <Windows.h>

#elif defined (__unix__) || (defined (__APPLE__) && defined (__MACH__))

extern "C"
{
# include <unistd.h>
}

#else 

# error Platform not supported

#endif


using namespace std;

namespace
{
#ifdef WIN32

  auto GetProcessId() -> decltype(::GetCurrentProcessId())
  {

    return ::GetCurrentProcessId();
  }

#else

  auto GetProcessId() -> decltype(::getpid())
  {
    return ::getpid();
  }

#endif

  static_assert(std::numeric_limits<decltype(GetProcessId())>::is_integer, "GetProcessId() must return an integer type");

}  // unnamed namespace


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

  ProcessToken GetProcessToken()
  {
    static_assert(sizeof(ProcessToken) - sizeof(GetProcessId()) >= 4, "ProcessToken needs to hold at least 4 bytes (32 bits) of random data");
    // note: looks like SQLite does not like "-" in savepoint names ...
    static_assert(!std::numeric_limits<decltype(GetProcessToken())>::is_signed, "Implementation detail: ProcessToken must not be signed");

    union ProcessTokenInitializer
    {
      ProcessToken             m_ProcessToken;

      struct
      {
        decltype(GetProcessId()) m_ProcessId;
        uint8_t                  m_RandomData[sizeof(ProcessToken) - sizeof(GetProcessId())];
      };
    };

    static_assert(sizeof(ProcessTokenInitializer) == sizeof(ProcessToken), "");
    static_assert((sizeof(ProcessTokenInitializer().m_ProcessId) + sizeof(ProcessTokenInitializer().m_RandomData)) == sizeof(ProcessToken), "");

    static Detail::RandomNumberGenerator<decltype(GetProcessToken())> randomNumberGenerator;

    ProcessTokenInitializer initializer;

    // initialize token with random data
    initializer.m_ProcessToken = randomNumberGenerator.Get();

    // copy process id into part of the process token
    initializer.m_ProcessId = GetProcessId();
    
    // copy resulting process token
    return initializer.m_ProcessToken;
  }
}
