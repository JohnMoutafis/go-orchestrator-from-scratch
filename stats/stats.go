package stats

import (
	"log"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
)

type Stats struct {
	MemStats  *mem.VirtualMemoryStat
	DiskStats *disk.UsageStat
	CpuStats  *cpu.TimesStat
	LoadStats *load.AvgStat
	TaskCount int
}

// Stats Helper
func (s *Stats) MemUsedKb() uint64 {
	return s.MemStats.Used
}

func (s *Stats) MemUsedPercent() uint64 {
	return uint64(s.MemStats.UsedPercent)
}

func (s *Stats) MemAvailableKb() uint64 {
	return s.MemStats.Available
}

func (s *Stats) MemTotalKb() uint64 {
	return s.MemStats.Total
}

func (s *Stats) DiskTotal() uint64 {
	return s.DiskStats.Total
}

func (s *Stats) DiskFree() uint64 {
	return s.DiskStats.Free
}

func (s *Stats) DiskUsed() uint64 {
	return s.DiskStats.Used
}

func (s *Stats) CpuUsage() (float64, float64, float64, float64) {

	idle := s.CpuStats.Idle + s.CpuStats.Iowait
	nonIdle := s.CpuStats.User + s.CpuStats.Nice + s.CpuStats.System + s.CpuStats.Irq + s.CpuStats.Softirq + s.CpuStats.Steal
	total := idle + nonIdle

	usagePercent := 0.00
	if total > 0 {
		usagePercent = (float64(total) - float64(idle)) / float64(total)
	}

	return usagePercent, idle, nonIdle, total
}

// Stat "Aggregator"
func GetStats() *Stats {
	return &Stats{
		MemStats:  GetMemoryInfo(),
		DiskStats: GetDiskInfo(),
		CpuStats:  GetCpuStats(),
		LoadStats: GetLoadAvg(),
	}
}

/**
* Originally the solution uses goprocinfo library, assuming that it will run in a linux system.
* That library does not work universally (ex on Mac), therefore it is replaced with gopsutil.
* Original methods used:
* * GetMemoryInfo See https://godoc.org/github.com/c9s/goprocinfo/linux#MemInfo
* * GetDiskInfo See https://godoc.org/github.com/c9s/goprocinfo/linux#Disk
* * GetCpuInfo See https://godoc.org/github.com/c9s/goprocinfo/linux#CPUStat
* * GetLoadAvg See https://godoc.org/github.com/c9s/goprocinfo/linux#LoadAvg
 */
func GetMemoryInfo() *mem.VirtualMemoryStat {
	mem_stats, err := mem.VirtualMemory()
	if err != nil {
		log.Printf("Error reading from /proc/meminfo")
		return &mem.VirtualMemoryStat{}
	}

	return mem_stats
}

func GetDiskInfo() *disk.UsageStat {
	disk_stats, err := disk.Usage("/")
	if err != nil {
		log.Printf("Error reading from /")
		return &disk.UsageStat{}
	}

	return disk_stats
}

func GetCpuStats() *cpu.TimesStat {
	stats, err := cpu.Times(false)
	if err != nil {
		log.Printf("Error reading from /proc/stat")
		return &cpu.TimesStat{}
	}

	return &stats[0]
}

func GetLoadAvg() *load.AvgStat {
	load_avg, err := load.Avg()
	if err != nil {
		log.Printf("Error reading from /proc/loadavg")
		return &load.AvgStat{}
	}

	return load_avg
}
