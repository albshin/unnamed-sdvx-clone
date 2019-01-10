#pragma once
#include "Beatmap.hpp"
#include <thread>
#include <mutex>
#include "Database.hpp"

struct SimpleHitStat
{
	// 0 = miss, 1 = near, 2 = crit, 3 = idle
	int8 rating;
	int8 lane;
	int32 time;
	int32 delta;
	// Hold state
	// This is the amount of gotten ticks in a hold sequence
	uint32 hold = 0;
	// This is the amount of total ticks in this hold sequence
	uint32 holdMax = 0;
};

struct ScoreIndex
{
	int32 id;
	int32 diffid;
	int32 score;
	int32 crit;
	int32 almost;
	int32 miss;
	float gauge;
	uint32 gameflags;
	Vector<SimpleHitStat> hitStats;
	uint64 timestamp;
};


// Single difficulty of a map
// a single map may contain multiple difficulties
struct DifficultyIndex
{
	// Id of this difficulty
	int32 id;
	// Id of the map that contains this difficulty
	int32 mapId;
	// Full path to the difficulty
	String path;
	// Last time the difficulty changed
	uint64 lwt;
	// Map metadata
	BeatmapSettings settings;
	// Map scores
	Vector<ScoreIndex*> scores;


};

// Map located in database
//	a map is represented by a single subfolder that contains map file
struct MapIndex
{
	// Id of this map
	int32 id;
	// Id of this map
	int32 selectId;
	// Full path to the map root folder
	String path;
	// List of difficulties contained within the map
	Vector<DifficultyIndex*> difficulties;
};

struct SearchState
{
	struct ExistingDifficulty
	{
		int32 id;
		uint64 lwt;
	};
	// Maps file paths to the id's and last write time's for difficulties already in the database
	Map<String, ExistingDifficulty> difficulties;
};

// Represents an event produced from a scan
//	a difficulty can be removed/added/updated
//	a BeatmapSettings structure will be provided for added/updated events
struct Event
{
	enum Action
	{
		Added,
		Removed ,
		Updated
	};
	Action action;
	String path;
	// Current lwt of file
	uint64 lwt;
	// Id of the map
	int32 id;
	// Scanned map data, for added/updated maps
	BeatmapSettings* mapData = nullptr;
};

class MapDatabase : public Unique
{
public:
	MapDatabase();
	~MapDatabase();

	// Checks the background scanning and actualized the current map database
	void Update();

	bool IsSearching() const;
	void StartSearching();
	void StopSearching();

	// Grab all the maps, with their id's
	Map<int32, MapIndex*> GetMaps();
	// Finds maps using the search query provided
	// search artist/title/tags for maps for any space separated terms
	Map<int32, MapIndex*> FindMaps(const String& search);
	Map<int32, MapIndex*> FindMapsByFolder(const String& folder);
	MapIndex* GetMap(int32 idx);

	void AddSearchPath(const String& path);
	void AddScore(const DifficultyIndex& diff, int score, int crit, int almost, int miss, float gauge, uint32 gameflags, Vector<SimpleHitStat> simpleHitStats, uint64 timestamp);
	void RemoveSearchPath(const String& path);


	Delegate<String> OnSearchStatusUpdated;
	// (mapId, mapIndex)
	Delegate<Vector<MapIndex*>> OnMapsRemoved;
	// (mapId, mapIndex)
	Delegate<Vector<MapIndex*>> OnMapsAdded;
	// (mapId, mapIndex)
	Delegate<Vector<MapIndex*>> OnMapsUpdated;
	// Called when all maps are cleared
	// (newMapList)
	Delegate<Map<int32, MapIndex*>> OnMapsCleared;

private:
	std::thread m_thread;
	bool m_searching = false;
	bool m_interruptSearch = false;
	Set<String> m_searchPaths;
	Database m_database;

	Map<int32, MapIndex*> m_maps;
	Map<int32, DifficultyIndex*> m_difficulties;
	Map<String, MapIndex*> m_mapsByPath;
	int32 m_nextMapId = 1;
	int32 m_nextDiffId = 1;
	String m_sortField = "title";

	SearchState m_searchState;

	List<Event> m_pendingChanges;
	std::mutex m_pendingChangesLock;

	static const int32 m_version = 10;

	void m_CleanupMapIndex();
	void m_CreateTables();
	void m_LoadInitialData();
	static void m_SortDifficulties(MapIndex* mapIndex);
	static void m_SortScores(DifficultyIndex* diffIndex);
	void m_SearchThread();
	void AddChange(Event change);
	List<Event> FlushChanges(size_t maxChanges = -1);
};