// Copyright (c) 2014 by Bertolt Mildner
// All rights reserved.

#include "Configuration.h"

#include <cassert>
#include <algorithm>
#include <set>
#include <map>

#include "Utils.h"

CONFIGURATION_BOOST_INCL_GUARD_BEGIN
#include <boost/format.hpp>
#include <boost/tokenizer.hpp>
#include <boost/random/random_device.hpp>
#include <boost/random/uniform_int_distribution.hpp>
CONFIGURATION_BOOST_INCL_GUARD_END

// TODO: maybe we schould define SQLITECPP_ENABLE_ASSERT_HANDLER !?1

using namespace std;

namespace 
{
  const string Table_Settings = "Settings";
  const string Table_Entries = "Entries";

  const std::string Table_Settings_Column_Name = "Name";
  const std::string Table_Settings_Column_Value = "Value";

  const std::string Table_Entries_Column_Id = "Id";
  const std::string Table_Entries_Column_Parent = "Parent";
  const std::string Table_Entries_Column_Name = "Name";
  const std::string Table_Entries_Column_Revision = "Revision";
  const std::string Table_Entries_Column_Type = "Type";
  const std::string Table_Entries_Column_Value = "Value";

  const std::string Table_Entries_Name_Index = "TableEntries_Name";
  const std::string Table_Entries_Parent_Index = "TableEntries_Parent";
  const std::string Table_Entries_Name_Parent_Index = "TableEntries_Name_Parent";

  const std::string Table_Entries_RootEntry = "Root";

  const std::string Setting_MajorVersion = "MajorVersion";
  const std::string Setting_MinorVersion = "MinorVersion";
  const std::string Setting_NameDelimeter = "NameDelimeter";

  wstring SQLiteDataTypeToStr(int type)
  {
    switch (type)
    {
      case SQLITE_INTEGER:
        return L"SQLITE_INTEGER";
        break;

      case SQLITE_FLOAT:
        return L"SQLITE_FLOAT";
        break;

      case SQLITE_BLOB:
        return L"SQLITE_BLOB";
        break;

      case SQLITE_NULL:
        return L"SQLITE_NULL";
        break;

      case SQLITE_TEXT:
        return L"SQLITE_TEXT";
        break;

      default:
        return (boost::wformat(L"Unknown SQLite data type (%1%)") % type).str();
     }
  }
}

namespace Configuration
{
  namespace Detail
  {
    class RandomNumberGenerator
    {
      public:
        RandomNumberGenerator()
          : m_RandomDevice(), m_Distribution(numeric_limits<Store::Integer>::min(), numeric_limits<Store::Integer>::max())
        {}

        inline Store::Integer Get()
        {
          return m_Distribution(m_RandomDevice);
        }

      private:
        boost::random::random_device                            m_RandomDevice;
        boost::random::uniform_int_distribution<Store::Integer> m_Distribution;
    };
  }

  const Store::Integer Store::CurrentMajorVersion = 1;
  const Store::Integer Store::CurrentMinorVersion = 0;

  const Store::String::value_type Store::DefaultNameDelimeter = L'.';

  const Store::ValueType        Store::DefaultEntryValueType = ValueType::Integer;
  const Store::DefaultEntryType Store::DefaultEntryValue = 0;


  // TODO: check if we should use SQLITE_OPEN_NOMUTEX instead of SQLITE_OPEN_FULLMUTEX and/or if we can be really multi-thread save with SQLITE_OPEN_FULLMUTEX!?
  Store::Store(const wstring& fileName, bool create, wchar_t nameDelimeter)
  : m_Database(WcharToUTF8(fileName), SQLITE_OPEN_FULLMUTEX | SQLITE_OPEN_READWRITE | (create ? SQLITE_OPEN_CREATE : 0)),
  m_DatabaseVersionMajor(0), m_DatabaseVersionMinor(0), m_Delimeter()
  {

    // set busy timeout
    m_Database.setBusyTimeout(15000);  // max. wait is 15sec

    // setup database basic database settings we can not change within a transaction
    m_Database.exec("PRAGMA auto_vacuum = FULL");
    m_Database.exec("PRAGMA synchronous = FULL");
    m_Database.exec("PRAGMA foreign_keys = TRUE");

    // open writeable transaction
    WriteableTransaction transaction(*this);

    // setup database basic database settings
    m_Database.exec("PRAGMA encoding           = \"UTF-8\"");
    m_Database.exec("PRAGMA foreign_keys       = TRUE");
    m_Database.exec("PRAGMA journal_mode       = DELETE");
    m_Database.exec("PRAGMA locking_mode       = NORMAL");
    m_Database.exec("PRAGMA recursive_triggers = TRUE");
    m_Database.exec("PRAGMA secure_delete      = TRUE");

    // create tables
    m_Database.exec("CREATE TABLE IF NOT EXISTS " + Table_Settings + "(" + Table_Settings_Column_Name +  " TEXT  PRIMARYKEY, " +
                                                                           Table_Settings_Column_Value + " BLOB"
                                                                  ")");

    m_Database.exec("CREATE TABLE IF NOT EXISTS " + Table_Entries + "(" +
                                                    Table_Entries_Column_Id +       " INTEGER  PRIMARY KEY, " +
                                                    Table_Entries_Column_Parent +   " INTEGER  NOT NULL, " +
                                                    Table_Entries_Column_Revision + " INTEGER  NOT NULL," +
                                                    Table_Entries_Column_Name +     " TEXT     NOT NULL, " +
                                                    Table_Entries_Column_Type +     " INTEGER  NOT NULL, " +
                                                    Table_Entries_Column_Value +    " BLOB"  // we need NULL to store an empty BLOB ....
                                                    ")");

    // create index
    m_Database.exec("CREATE INDEX IF NOT EXISTS " + Table_Entries_Name_Index + " ON " + Table_Entries + "(" + Table_Entries_Column_Name + ")");

    m_Database.exec("CREATE INDEX IF NOT EXISTS " + Table_Entries_Parent_Index + " ON " + Table_Entries + "(" + Table_Entries_Column_Parent + ")");

    m_Database.exec("CREATE UNIQUE INDEX IF NOT EXISTS " + Table_Entries_Name_Parent_Index + " ON " +
                                                            Table_Entries + "(" + Table_Entries_Column_Name + "," + Table_Entries_Column_Parent + ")");
  
    // check DB inegrity
    m_Database.exec("PRAGMA integrity_check");
    m_Database.exec("PRAGMA foreign_key_check");

    // get config and do minimal sanity-check on data in db
    GetAndCheckConfiguration(nameDelimeter);
    CheckOrSetRootEntry();

    transaction.Commit();
  }

  Store::~Store() noexcept
  {
  }

  void Store::GetAndCheckConfiguration(wchar_t nameDelimeter)
  {
    // open writeable transaction
    WriteableTransaction transaction(*this);

    // get and check version information
    bool majorVersionExists = SettingExists(Setting_MajorVersion);
    bool minorVersionExists = SettingExists(Setting_MinorVersion);

    if (!majorVersionExists || !minorVersionExists)
    {
      if (majorVersionExists || minorVersionExists)  // check for partial version information
      {
        throw ExceptionImpl<InvalidConfiguration>(L"Partial version information found");
      }

      SetSetting(Setting_MajorVersion, CurrentMajorVersion);
      SetSetting(Setting_MinorVersion, CurrentMinorVersion);
    }

    m_DatabaseVersionMajor = GetSettingInt(Setting_MajorVersion);
    m_DatabaseVersionMinor = GetSettingInt(Setting_MinorVersion);

    if (m_DatabaseVersionMajor > CurrentMajorVersion)
    {
      throw ExceptionImpl<VersionNotSupported>(
        (boost::wformat(L"Database version %1%.%2%") % m_DatabaseVersionMajor % m_DatabaseVersionMinor).str());
    }

    // get and check name delimeter
    if (!SettingExists(Setting_NameDelimeter))
    {
      SetSetting(Setting_NameDelimeter, String(1, nameDelimeter));
      m_Delimeter = DefaultNameDelimeter;
    }
    else
    {
      String delimeter = GetSettingStr(Setting_NameDelimeter);

      if (delimeter.size() != 1)
      {
        throw ExceptionImpl<InvalidConfiguration>(
          (boost::wformat(L"Invalid value for %1% setting (%2%)") % UTF8ToWchar(Setting_NameDelimeter) % delimeter).str());
      }

      m_Delimeter = delimeter.at(0);

      if (m_Delimeter != nameDelimeter)
      {
        throw ExceptionImpl<DelimeterMissmatch>((boost::wformat(L"Expected delimeter \"%1%\" but found \"%2%\" in configuration.") % nameDelimeter % m_Delimeter).str());
      }
    }

    transaction.Commit();
  }

  void Store::CheckOrSetRootEntry()
  {
    WriteableTransaction transaction(*this);

    static const string Statement1 = "SELECT " + Table_Entries_Column_Id + "," +
                                                 Table_Entries_Column_Parent + "," +
                                                 Table_Entries_Column_Type + "," +
                                                 Table_Entries_Column_Name + "," +
                                                 Table_Entries_Column_Value +
                                                   " FROM " + Table_Entries + " WHERE (" + Table_Entries_Column_Id + " = 0)";
    auto root = GetStatement(Statement1);

    if (!root->executeStep())
    {  // root entry not found
      // make sure the table is empty if there is no root entry!
      static const string Statement2 = "SELECT COUNT(" + Table_Entries_Column_Id + ") FROM " + Table_Entries;
      auto count = GetStatement(Statement2);

      if (!count->executeStep())
      {
        throw ExceptionImpl<InvalidQuery>(L"Failed to query count of entries in table " + UTF8ToWchar(Table_Entries));
      }
      
      if (count->getColumn(0).getInt64() != 0)
      {
        throw ExceptionImpl<RootEntryMissing>(L"Missing root entry in non-empty table " + UTF8ToWchar(Table_Entries));
      }

      // create new root entry
      static const string Statement3 = "INSERT INTO " + Table_Entries + " (" + Table_Entries_Column_Id + "," +
                                                                               Table_Entries_Column_Parent + "," +
                                                                               Table_Entries_Column_Revision + "," +
                                                                               Table_Entries_Column_Type + "," +
                                                                               Table_Entries_Column_Name + "," +
                                                                               Table_Entries_Column_Value + ") "
                                                                                 "VALUES (0, 0, 0, ?1, ?2, ?3)";
      auto newRoot = GetStatement(Statement3);
      
      newRoot->bind(1, static_cast<Integer>(DefaultEntryValueType));
      newRoot->bind(2, Table_Entries_RootEntry);
      newRoot->bind(3, DefaultEntryValue);

      if (newRoot->exec() != 1)
      {
        throw ExceptionImpl<InvalidInsert>(L"Failed to insert new root entry into table " + UTF8ToWchar(Table_Entries));
      }
    }
    else
    {
      if ((root->getColumn(0).getInt64() != 0) ||
          (root->getColumn(1).getInt64() != 0) ||
          (root->getColumn(2).getInt64() != static_cast<Integer>(DefaultEntryValueType)) ||
          (root->getColumn(3).getText()  != Table_Entries_RootEntry) ||
          (root->getColumn(4).getInt64() != 0))
      {
        throw ExceptionImpl<InvalidRootEntry>(L"Root entry contains invalid data");
      }
    }

    transaction.Commit();
  }

  void Store::TraverseChildren(Integer id, std::function<void(Integer)> func) const
  {
    IdList children = GetChildEntries(id);

    for (auto child : children)
    {
      func(child);

      TraverseChildren(child, func);
    }
  }

  void Store::CheckDataConsistency() const
  {
    ReadOnlyTransaction transaction(*this);

    // find all entries with a delimeter in name
    {
      vector<Integer> badEntries;

      static const string Statement1 = "SELECT DISTINCT " + Table_Entries_Column_Name + " FROM " + Table_Entries;
      auto stm = GetStatement(Statement1);

      while (stm->executeStep())
      {      
        string name = stm->getColumn(0).getText();

        if (UTF8ToWchar(name).find(m_Delimeter) != String::npos)
        {
          static const string Statement2 = "SELECT " + Table_Entries_Column_Id + " FROM " + Table_Entries +
                                             " WHERE " + Table_Entries_Column_Name + " = ?1";
          auto stm2 = GetStatement(Statement2);

          stm2->bind(1, name);

          while (stm2->executeStep())
          {
            badEntries.emplace_back(stm2->getColumn(0).getInt64());
          }
        }
      }

      if (!badEntries.empty())
      {
        wstring msg = (boost::wformat(L"Found following %1% entries with name delimeter in name: ") % badEntries.size()).str();
      
        for (auto i : badEntries)
        {
          msg += (boost::wformat(L" %2%, ") % i).str();
        }

        msg.resize(msg.size() - 2);

        throw ExceptionImpl<InvalidEntryNameFound>(msg);
      }
    }

    // check linking of all entries!
    {
      // get id of ALL entries except root entry
      set<Integer> entries;

      static const string Statement3 = "SELECT " + Table_Entries_Column_Id + " FROM " + Table_Entries + " WHERE " + Table_Entries_Column_Id + " != 0";
      auto stm = GetStatement(Statement3);

      while (stm->executeStep())
      {
        if (!entries.emplace(stm->getColumn(0).getInt64()).second)  // ids must be unique
        {
          throw ExceptionImpl<EntryIdNotUnique>((boost::wformat(L"There are multiple entries with id: %1%") % stm->getColumn(0).getInt64()).str());
        }
      }

      map<Integer, size_t> brokenLinks;

      // traverse all entries from root according to linking and remove them from our entries set
      TraverseChildren(0, [&entries, &brokenLinks](Integer id)
      { 
        if (entries.erase(id) != 1) 
        {
          brokenLinks.insert(make_pair(id, 0)).first->second++;
        }
      });
      
      if (!brokenLinks.empty())
      {
        String ids;

        for (auto id : brokenLinks)
        {
          if (!ids.empty())
          {
            ids += L' ';
          }

          ids += (boost::wformat(L"(id: %1%, count: %2%)") % id.first % id.second).str();
        }

        throw ExceptionImpl<InvalidEntryLinking>((boost::wformat(L"Found %1% entries with broken linking: %2%") % entries.size() % ids).str());
      }

      if (!entries.empty())
      {
        String ids;

        for (auto id : entries)
        {
          if (!ids.empty())
          {
            ids += L' ';
          }

          ids += (boost::wformat(L"%1%") % id).str();
        }

        throw ExceptionImpl<AbandonedEntry>((boost::wformat(L"Found %1% abandoned entries: %2%") % entries.size() % ids).str());
      }
    }
  }

  Store::Integer Store::RepairDataConsistency()
  {
    WriteableTransaction transaction(*this);

    // TODO: move all dangling / damaged entries into lost&found table?

    transaction.Commit();

    return 0;
  }



  Store::String::value_type Store::GetNameDelimeter() const noexcept
  {
    return m_Delimeter;
  }

  bool Store::IsValidName(const String& name, String::value_type delimeter)
  {
    // must not be empty
    if (name.empty())
    {
      return false;
    }

    // must not start or end with delimeter
    if ((name.front() == delimeter) || (name.back() == delimeter))
    {
      return false;
    }

    // must not contain multiple consecutive delimeters
    if (name.find(String(2, delimeter)) != String::npos)
    {
      return false;
    }

    return true;
  }

  Store::Path Store::ParseName(const String& name) const
  {
    if (!IsValidName(name))
    {
      throw ExceptionImpl<InvalidName>(L"Invalid name: " + name);
    }
    
    String delimeter;
    delimeter += m_Delimeter;    

    using Tok = boost::tokenizer<boost::char_separator<String::value_type>, String::const_iterator, String>;

    Tok tok(name, boost::char_separator<String::value_type>(delimeter.c_str(), 0));

    return Path(tok.begin(), tok.end());
  }

  Store::String Store::PathToName(const Path& path) const
  {
    String str;
    String::value_type delim = m_Delimeter;

    for_each(path.begin(), path.end(), [=, &str](const String& name) { if (!str.empty()) str += delim; str += name; });

    return str;
  }

  Store::String Store::ValueTypeToString(ValueType type) const
  {
    switch (type)
    {
      case ValueType::Integer:
        return L"Integer";
        break;

      case ValueType::String:
        return L"String";
        break;

      case ValueType::Binary:
        return L"Binary";
        break;

      default:
        return (boost::wformat(L"Unknown type (%1%)") % static_cast<Integer>(type)).str();
    }
  }

  bool Store::GetEntryId(IdList& idPath, const String& name, Integer parent) const
  {
    assert(m_Transaction.lock());

    static const string Statement = "SELECT " + Table_Entries_Column_Id + " FROM " + Table_Entries +
                                      " WHERE " + Table_Entries_Column_Name + " = ?1 AND " +
                                                  Table_Entries_Column_Parent + " = ?2";
    auto stm = GetStatement(Statement);

    stm->bind(1, WcharToUTF8(name));
    stm->bind(2, parent);

    if (!stm->executeStep())
    {
      return false;
    }

    idPath.push_back(stm->getColumn(0).getInt64());

    assert(!stm->executeStep());

    return true;
  }

  bool Store::GetEntryId(IdList& idPath, Path::const_iterator& lastValid, const Path& path, Integer parent) const
  {
    lastValid = path.end();
    idPath.clear();

    for (auto iter = path.begin(); iter != path.end(); iter++)
    {
      if (!GetEntryId(idPath, *iter, !idPath.empty() ? idPath.back() : parent))
      {
        return false;
      }

      lastValid = iter;
    }

    assert(idPath.size() > 0);

    return true;
  }

  bool Store::GetEntryId(IdList& idPath, const Path& path, Integer parent) const
  {
    Path::const_iterator iter;

    return GetEntryId(idPath, iter, path, parent);
  }

  Store::IdList Store::GetEntryId(const Path& path, Integer parent) const
  {
    IdList idPath;

    if (!GetEntryId(idPath, path, parent))
    {
      throw ExceptionImpl<EntryNotFound>(L"Entry not found: " + PathToName(path));
    }

    assert(!idPath.empty());

    return idPath;
  }

  Store::IdList Store::GetEntryId(const String& name, Integer parent) const
  {
    Path path;

    path.emplace_back(name);

    return GetEntryId(path, parent);
  }

  bool Store::Exists(const Path& path) const
  {
    IdList id;
    
    return GetEntryId(id, path);
  }

  bool Store::Exists(const String& name) const
  {
    ReadOnlyTransaction transaction(*this);

    return Exists(ParseName(name));
  }

  Store::Integer Store::GetEntryRevision(Integer id) const
  {
    assert(m_Transaction.lock());

    // duplicate statement in UpdateRevision(), reason is performance !
    static const string Statement = "SELECT " + Table_Entries_Column_Revision + " FROM " + Table_Entries + " WHERE " + Table_Entries_Column_Id + " = ?1";

    auto stm = GetStatement(Statement);

    stm->bind(1, id);

    if (!stm->executeStep())
    {
      throw ExceptionImpl<InvalidQuery>((boost::wformat(L"Failed to query revision of entry: %1%") % id).str());
    }

    Integer revision = stm->getColumn(0).getInt64();

    assert(!stm->executeStep());

    return revision;
  }

  Store::Revision Store::GetRevision(const String& name) const
  {
    ReadOnlyTransaction transaction(*this);

    Integer id = !name.empty() ? GetEntryId(ParseName(name)).back() : 0;

    return Revision(id, GetEntryRevision(id));
  }

  void Store::UpdateRevision(IdList::const_iterator first, IdList::const_iterator last)  
  {
    assert(m_Transaction.lock() && m_WriteableTransaction);

    // duplicate statement in GetEntryRevision(), reason is performance !
    static const string StatementGet = "SELECT " + Table_Entries_Column_Revision + " FROM " + Table_Entries + " WHERE " + Table_Entries_Column_Id + " = ?1";

    static const string StatementUpdate = "UPDATE " + Table_Entries + " SET " + Table_Entries_Column_Revision + " = ?2" +
                                                                        " WHERE " + Table_Entries_Column_Id + " = ?1";
    auto get = GetStatement(StatementGet);
    auto update = GetStatement(StatementUpdate);

    // get root revision
    get->bind(1, 0);

    if (!get->executeStep())
    {
      throw ExceptionImpl<InvalidQuery>(L"Failed to query revision of entry: 0");
    }

    Integer revision = get->getColumn(0).getInt64();

    assert(!get->executeStep());

    // update root revision
    update->bind(1, 0);
    update->bind(2, ++revision);
    update->exec();

    // update entries from idPath
    while (first != last)
    {
      get->reset();
      update->reset();

      // get revision
      get->bind(1, *first);

      if (!get->executeStep())
      {
        throw ExceptionImpl<InvalidQuery>((boost::wformat(L"Failed to query revision of entry: %1%") % *first).str());
      }

      revision = get->getColumn(0).getInt64();

      assert(!get->executeStep());

      // update revision
      update->bind(1, *first);
      update->bind(2, ++revision);
      update->exec();

      first++;
    }
  }

  void Store::SetEntry(const IdList& idPath, ValueType type, const ValueBinder& bindValue)
  {
    assert(m_Transaction.lock() && m_WriteableTransaction);

    static const string Statement = "UPDATE " + Table_Entries + " SET " + Table_Entries_Column_Type + " = ?1 , " +
                                                                          Table_Entries_Column_Value + " = ?2 " +
                                                                            "WHERE " + Table_Entries_Column_Id + " = ?3";
    auto stm = GetStatement(Statement);

    stm->bind(1, static_cast<Integer>(type));
    bindValue(2, *stm);
    stm->bind(3, idPath.back());

    stm->exec();

    UpdateRevision(idPath.begin(), idPath.end());
  }

  void Store::SetEntry(const String& name, ValueType type, const ValueBinder& bindValue)
  {
    WriteableTransaction transaction(*this);

    SetEntry(GetEntryId(ParseName(name)), type, bindValue);

    transaction.Commit();
  }

  void Store::Set(const String& name, const String& value)
  {
    SetEntry(name, ValueType::String, [&value](int index, SQLite::Statement& stm) { stm.bind(index, WcharToUTF8(value)); });
  }

  void Store::Set(const String& name, Integer value)
  {
    SetEntry(name, ValueType::Integer, [&value](int index, SQLite::Statement& stm) { stm.bind(index, value); });
  }

  void Store::Set(const String& name, const Binary& value)
  {
    SetEntry(name, ValueType::Binary, [&value](int index, SQLite::Statement& stm) { stm.bind(index, value.data(), value.size()); });
  }

  Store::Integer Store::GetRandomRevision()
  {
    // TODO: thread safety!!
    if (!m_RandomNumberGenerator)
    {
      m_RandomNumberGenerator = make_unique<Detail::RandomNumberGenerator>();
    }

    return m_RandomNumberGenerator->Get();
  }

  void Store::CreateEntry(Integer parent, const String& name, ValueType type, const ValueBinder& bindValue)
  {
    assert(m_Transaction.lock() && m_WriteableTransaction);

    static const string Statement = "INSERT INTO " + Table_Entries + " (" + Table_Entries_Column_Name + "," +
                                                                            Table_Entries_Column_Parent + "," +
                                                                            Table_Entries_Column_Type + "," +
                                                                            Table_Entries_Column_Revision + "," +
                                                                            Table_Entries_Column_Value + ") " +
                                                                              "VALUES (?1, ?2, ?3, ?4, ?5)";
    auto stm = GetStatement(Statement);
  
    stm->bind(1, WcharToUTF8(name));
    stm->bind(2, parent);
    stm->bind(3, static_cast<Integer>(type));
    stm->bind(4, GetRandomRevision());
    bindValue(5, *stm);

    stm->exec();
  }

  void Store::CreateEntry(IdList parentPath, Path::const_iterator first, const Path::const_iterator& last, ValueType type, const ValueBinder& bindValue)
  {
    while (first != last)
    {
      if ((first + 1) == last)
      {
        // create new entry
        CreateEntry(!parentPath.empty() ? parentPath.back() : 0, *first, type, bindValue);
      }
      else
      {
        // create intermediate entry with defualt values
        CreateEntry(!parentPath.empty() ? parentPath.back() : 0, *first, DefaultEntryValueType,
                    [](int index, SQLite::Statement& stm) { stm.bind(index, DefaultEntryValue); });

        // get id of new entry and set it as new parent
        parentPath.push_back(GetEntryId(*first, !parentPath.empty() ? parentPath.back() : 0).back());
      }

      first++;
    }
  }

  void Store::CreateEntry(const Path& path, ValueType type, const ValueBinder& bindValue)
  {
    WriteableTransaction transaction(*this);

    IdList idPath;
    auto   lastValid = path.end();
  
    assert(!path.empty());

    // get id of parent for our new entry
    // GetEntryId() will stop at the last valid/found entry in the path
    if (!GetEntryId(idPath, lastValid, path))
    {
      assert((idPath.empty()) && (lastValid == path.end()) || ((!idPath.empty()) && (lastValid != path.end())));

      auto iter = (lastValid == path.end()) ? path.begin() : ++lastValid;  // handle case "no valid name in path found"

      assert(iter != path.end());

      // create missing part of path + our new entry, insert default values in new entries, idPath contains existing parent path
      CreateEntry(idPath, iter, path.end(), type, bindValue);

      // update revision in parent path entries
      UpdateRevision(idPath.begin(), idPath.end());
    }
    else
    {
      throw ExceptionImpl<NameAlreadyExists>(L"Name alreday exists: " + PathToName(path));
    }

    transaction.Commit();
  }

  void Store::Create(const String& name, const String& value)
  {
    CreateEntry(ParseName(name), ValueType::String, [&value](int index, SQLite::Statement& stm) { stm.bind(index, WcharToUTF8(value)); });
  }

  void Store::Create(const String& name, Integer value)
  {
    CreateEntry(ParseName(name), ValueType::Integer, [&value](int index, SQLite::Statement& stm) { stm.bind(index, value); });
  }

  void Store::Create(const String& name, const Binary& value)
  {
    CreateEntry(ParseName(name), ValueType::Binary, [&value](int index, SQLite::Statement& stm) { stm.bind(index, value.data(), value.size()); });
  }


  void Store::SetOrCreate(const Path& path, ValueType type, const ValueBinder& bindValue)
  {
    WriteableTransaction transaction(*this);

    IdList idPath;
    auto lastValid = path.end();

    assert(!path.empty());

    // get id of parent for our new entry
    // GetEntryId() will stop at the last valid/found entry in the path
    if (!GetEntryId(idPath, lastValid, path))
    {
      assert((idPath.empty()) && (lastValid == path.end()) || ((!idPath.empty()) && (lastValid != path.end())));

      auto iter = (lastValid == path.end()) ? path.begin() : ++lastValid;  // handle case "no valid name in path found"

      assert(iter != path.end());

      // create missing part of path + our new entry, insert default values in new entries, idPath contains existing parent path
      CreateEntry(idPath, iter, path.end(), type, bindValue);

      // update revision in parent path entries
      UpdateRevision(idPath.begin(), idPath.end());
    }
    else
    {
      // set existing entry
      SetEntry(idPath, type, bindValue);
    }

    transaction.Commit();
  }

  void Store::SetOrCreate(const String& name, const String& value)
  {
    SetOrCreate(ParseName(name), ValueType::String, [&value](int index, SQLite::Statement& stm) { stm.bind(index, WcharToUTF8(value)); });
  }

  void Store::SetOrCreate(const String& name, Integer value)
  {
    SetOrCreate(ParseName(name), ValueType::Integer, [&value](int index, SQLite::Statement& stm) { stm.bind(index, value); });
  }

  void Store::SetOrCreate(const String& name, const Binary& value)
  {
    SetOrCreate(ParseName(name), ValueType::Binary, [&value](int index, SQLite::Statement& stm) { stm.bind(index, value.data(), value.size()); });
  }

  void Store::GetEntryValue(const Path& path, ValueType type, const ValueGetter& getValue) const
  {
    ReadOnlyTransaction transaction(*this);

    Integer id = GetEntryId(path).back();

    if (GetEntryType(id) != type)
    {
      throw ExceptionImpl<WrongValueType>((boost::wformat(L"Expected value type %1% for entry %2% but found: %3%") % ValueTypeToString(type) % PathToName(path) % ValueTypeToString(GetEntryType(id))).str());
    }

    static const string Statement = "SELECT " + Table_Entries_Column_Value + " FROM " + Table_Entries + " WHERE " + Table_Entries_Column_Id + " = ?1";
    auto stm = GetStatement(Statement);

    stm->bind(1, id);

    if (!stm->executeStep())
    {
      throw ExceptionImpl<InvalidQuery>(L"Failed to query value of entry: " + PathToName(path));
    }
    
    getValue(*stm);

    assert(!stm->executeStep());
  }

  Store::String Store::GetString(const String& name) const
  {
    String value;

    GetEntryValue(ParseName(name), ValueType::String, [&value](SQLite::Statement& stm) { value = UTF8ToWchar(stm.getColumn(0).getText()); });

    return value;
  }

  Store::Integer Store::GetInteger(const String& name) const
  {
    Integer value;

    GetEntryValue(ParseName(name), ValueType::Integer, [&value](SQLite::Statement& stm) { value = stm.getColumn(0).getInt64(); });

    return value;
  }

  Store::Binary Store::GetBinary(const String& name) const
  {
    Binary value;

    GetEntryValue(ParseName(name), ValueType::Binary, [&value](SQLite::Statement& stm) { if (!stm.isColumnNull(0)) 
                                                                                         {
                                                                                           value.resize(stm.getColumn(0).size()); 
                                                                                           memcpy(value.data(), stm.getColumn(0).getBlob(), value.size()); 
                                                                                         }
                                                                                       });

    return value;
  }

  bool Store::HasChild(Integer parent) const
  {
    assert(m_Transaction.lock());

    static const string Statement = "SELECT COUNT(" + Table_Entries_Column_Id + ") FROM " + Table_Entries + " WHERE " + Table_Entries_Column_Parent + " = ?1";
    auto stm = GetStatement(Statement);

    stm->bind(1, parent);

    if (!stm->executeStep())
    {
      throw ExceptionImpl<InvalidQuery>((boost::wformat(L"Failed to query number of childs for: %1%") % parent).str());
    }

    Integer count = stm->getColumn(0).getInt64();

    if (parent == 0)
    {
      assert(count > 0);
      count--;
    }

    assert(!stm->executeStep());

    return count > 0;
  }

  bool Store::HasChild(const String& name) const
  {
    ReadOnlyTransaction transaction(*this);

    return HasChild(name.empty() ? 0 : GetEntryId(ParseName(name)).back());
  }


  Store::IdList Store::GetChildEntries(Integer parent) const
  {
    assert(m_Transaction.lock());

    static const string Statement = "SELECT " + Table_Entries_Column_Id + " FROM " + Table_Entries + " WHERE " + Table_Entries_Column_Parent + " = ?1 AND " + Table_Entries_Column_Id + " != 0";
    auto stm = GetStatement(Statement);

    stm->bind(1, parent);

    IdList ids;

    while (stm->executeStep())
    {
      ids.emplace_back(stm->getColumn(0).getInt64());
    }

    return ids;
  }

  Store::Children Store::GetChildren(Integer parent) const
  {
    assert(m_Transaction.lock());

    static const string Statement = "SELECT " + Table_Entries_Column_Name + " FROM " + Table_Entries + " WHERE " + Table_Entries_Column_Parent + " = ?1 AND " + Table_Entries_Column_Id + " != 0";
    auto stm = GetStatement(Statement);

    stm->bind(1, parent);

    Children children;

    while (stm->executeStep())
    {
      children.emplace_back(UTF8ToWchar(stm->getColumn(0).getText()));
    }

    return children;
  }

  Store::Children Store::GetChildren(const String& name) const
  {
    ReadOnlyTransaction transaction(*this);

    return GetChildren(name.empty() ? 0 : GetEntryId(ParseName(name)).back());
  }

  Store::ValueType Store::GetType(const String& name) const
  {
    return GetEntryType(ParseName(name));
  }

  bool Store::IsInteger(const String& name) const
  {
    return (GetEntryType(ParseName(name)) == ValueType::Integer);
  }

  bool Store::IsString(const String& name) const
  {
    return (GetEntryType(ParseName(name)) == ValueType::String);
  }

  bool Store::IsBinary(const String& name) const
  {
    return (GetEntryType(ParseName(name)) == ValueType::Binary);
  }

  Store::ValueType Store::GetEntryType(const Path& path) const
  {
    ReadOnlyTransaction transcation(*this);

    return GetEntryType(GetEntryId(path).back());
  }

  Store::ValueType Store::GetEntryType(Integer id) const
  {
    assert(m_Transaction.lock());

    static const string Statement = "SELECT " + Table_Entries_Column_Type + " FROM " + Table_Entries + " WHERE " + Table_Entries_Column_Id + " = ?1";
    auto stm = GetStatement(Statement);

    stm->bind(1, id);

    if (!stm->executeStep())
    {
      ExceptionImpl<InvalidQuery>((boost::wformat(L"Failed to query value type for: %1%") % id).str());
    }

    ValueType type = static_cast<ValueType>(stm->getColumn(0).getInt64());
    
    switch (type)
    {
      case ValueType::Integer:
      case ValueType::String:
      case ValueType::Binary:
        break;

      default: throw ExceptionImpl<UnknownEntryType>((boost::wformat(L"Entry %1% has unknown value type: %2%") % id % static_cast<Integer>(type)).str());
    }

    assert(!stm->executeStep());

    return type;
  }

  bool Store::TryDeleteEntryImpl(Integer id, bool recursive)
  {
    assert(id != 0);

    if (recursive)
    {
      IdList childs = GetChildEntries(id);

      for (auto child : childs)
      {
        TryDeleteEntryImpl(child, recursive);
      }
    }
    else
    {
      if (HasChild(id))
      {
        return false;
      }
    }

    assert(m_Transaction.lock());

    static const string Statement = "DELETE FROM " + Table_Entries + " WHERE " + Table_Entries_Column_Id + " = ?1";
    auto stm = GetStatement(Statement);

    stm->bind(1, id);

    stm->exec();

    return true;
  }

  bool Store::TryDeleteEntry(const IdList& idPath, bool recursive)
  {
    assert(!idPath.empty());

    if (TryDeleteEntryImpl(idPath.back(), recursive))
    {
      // update revision of parent entries
      UpdateRevision(idPath.begin(), idPath.begin() + (idPath.size() - 1));

      return true;
    }

    return false;
  }

  bool Store::TryDelete(const String& name, bool recursive)
  {
    WriteableTransaction transaction(*this);

    IdList idPath;

    if (!GetEntryId(idPath, ParseName(name)))
    {
      return false;  // entry not found
    }

    if (!TryDeleteEntry(idPath, recursive))
    {
      return false;  // has children and recursive == false
    }

    transaction.Commit();

    return true;
  }

  void Store::Delete(const String& name, bool recursive)
  {
    WriteableTransaction transaction(*this);

    if (!TryDeleteEntry(GetEntryId(ParseName(name)), recursive))
    {
      throw ExceptionImpl<HasChildEntry>(L"Faild to delete due to existing child entries: " + name);
    }

    transaction.Commit();
  }


  std::shared_ptr<SQLite::Transaction> Store::GetTransaction(bool writeable) const
  {
    shared_ptr<SQLite::Transaction> transaction(m_Transaction.lock());

    if (transaction)
    {
      if (writeable && !m_WriteableTransaction)
      {
        throw ExceptionImpl<InvalidTransaction>(L"There is already a non-writeable transaction");
      }

      // return existing transaction
      return transaction;
    }
    else
    {
      // open new transcation
      transaction.reset(new SQLite::Transaction(m_Database, writeable ? SQLite::Transaction::TransactionType::Immediate :
                                                                        SQLite::Transaction::TransactionType::Deferred));

      m_Transaction = transaction;
      m_WriteableTransaction = writeable;

      return transaction;
    }
  }

  bool Store::SettingExists(const string& name) const
  {
    assert(m_Transaction.lock());

    static const string Statement = "SELECT 1 FROM " + Table_Settings + " WHERE " + Table_Settings_Column_Name + " = ?";
    auto stm = GetStatement(Statement);

    stm->bind(1, name);

    return stm->executeStep();
  }

  Store::SettingType Store::GetSettingType(const string& name) const
  {
    int type = GetSetting(name)->getColumn(0).getType();

    switch (type)
    {
      case SQLITE_INTEGER:
        return SettingType::Integer;
        break;

      case SQLITE_TEXT:
        return SettingType::String;
        break;

      case SQLITE_BLOB:
        return SettingType::Binary;
        break;

      default:                
        throw ExceptionImpl<UnknownDataType>(
          (boost::wformat(L"Unknown data type (%1%) for setting %2%") % type % UTF8ToWchar(name)).str());
    }
  }

  template <typename Value>
  void Store::SetSettingImpl(const string& name, const Value& value)
  {
    assert(m_Transaction.lock() && m_WriteableTransaction);

    static const string Statement = "INSERT OR REPLACE INTO " + Table_Settings + " VALUES (?1, ?2)";
    auto stm = GetStatement(Statement);

    stm->bind(1, name);
    stm->bind(2, value);

    stm->exec();
  }

  void Store::SetSettingImpl(const string& name, const Store::Binary& value)
  {
    assert(m_Transaction.lock() && m_WriteableTransaction);

    static const string Statement = "INSERT OR REPLACE INTO " + Table_Settings + " VALUES (?1, ?2)";
    auto stm = GetStatement(Statement);

    stm->bind(1, name);
    stm->bind(2, value.data(), value.size());

    stm->exec();
  }

  void Store::SetSetting(const string& name, Store::Integer value)
  {
    SetSettingImpl(name, value);
  }

  void Store::SetSetting(const string& name, const Store::String& value)
  {
    SetSettingImpl(name, WcharToUTF8(value));
  }

  void Store::SetSetting(const string& name, const Store::Binary& value)
  {
    SetSettingImpl(name, value);
  }

  // TODO: refactor to use valueGetter functor
  unique_ptr<SQLite::Statement> Store::GetSetting(const std::string& name, int type) const
  {
    assert(m_Transaction.lock());

    static const string Statement = "SELECT " + Table_Settings_Column_Value + " FROM " + Table_Settings +
                                                                              " WHERE " + Table_Settings_Column_Name + " = ?1";
    unique_ptr<SQLite::Statement> stm = make_unique<SQLite::Statement>(m_Database, Statement);

    stm->bind(1, name);

    if (!stm->executeStep())
    {
      throw ExceptionImpl<SettingNotFound>(L"Setting " + UTF8ToWchar(name) + L" not found");
    }

    assert(stm->getColumnCount() == 1);

    if (type != -1)
    {
      int actualType = stm->getColumn(0).getType();

      if (actualType != type)
      {
        throw ExceptionImpl<DataTypeMissmatch>(
          (boost::wformat(L"Data type missmatch: setting %1% has type %2%, expected %3%")
          % UTF8ToWchar(name) % SQLiteDataTypeToStr(actualType) % SQLiteDataTypeToStr(type)).str());
      }
    }

    return stm;
  }

  Store::Integer Store::GetSettingInt(const string& name) const
  {
    return GetSetting(name, SQLITE_INTEGER)->getColumn(0).getInt64();
  }

  Store::String Store::GetSettingStr(const string& name) const
  {
    return UTF8ToWchar(GetSetting(name, SQLITE_TEXT)->getColumn(0).getText());
  }

  Store::Binary Store::GetSettingBin(const string& name) const
  {
    unique_ptr<SQLite::Statement> stm(GetSetting(name, SQLITE_BLOB));

    if (stm->isColumnNull(0))
    {
      return Binary();
    }
    else
    {
      return Binary(reinterpret_cast<const uint8_t*>(stm->getColumn(0).getBlob()), 
                    reinterpret_cast<const uint8_t*>(stm->getColumn(0).getBlob()) + stm->getColumn(0).getBytes());
    }
  }

  Store::CachedStatement Store::GetStatement(const std::string& statementText) const
  {
    StatementCache::const_iterator iter = m_StatementCache.find(statementText);
    if (iter == m_StatementCache.end())
    {
      CachedStatement stm = make_shared<CachedStatement::element_type>(m_Database, statementText);

      m_StatementCache.insert(make_pair(statementText, stm));

      return stm;
    }
    else
    {
      iter->second->reset();

      return iter->second;
    }
  }

  ReadOnlyTransaction::ReadOnlyTransaction(const Store& store)
  : m_Transaction(store.GetTransaction(false))
  {
  }

  ReadOnlyTransaction::~ReadOnlyTransaction()
  {
  }

  WriteableTransaction::WriteableTransaction(Store& store)
  : m_Commited(false), m_Transaction(store.GetTransaction(true))
  {
    if (!m_Transaction.unique())
    {
      m_Transaction->SetSavepoint((boost::format("Configuration_Stor_%1%") % static_cast<void*>(this)).str());
    }
  }

  WriteableTransaction::~WriteableTransaction()
  {
    try
    {
      if (!m_Commited && !m_Transaction.unique())
      {
        m_Transaction->RollbackSavepoint((boost::format("Configuration_Stor_%1%") % static_cast<void*>(this)).str());
      }
    }
    catch (...)
    {
      // TODO: trace/log error
    }
  }

  void WriteableTransaction::Commit()
  {
    if (!m_Transaction.unique())
    {
      m_Transaction->ReleaseSavepoint((boost::format("Configuration_Stor_%1%") % static_cast<void*>(this)).str());
    }
    else
    {
      m_Transaction->commit();
    }

    m_Commited = true;
  }

}
