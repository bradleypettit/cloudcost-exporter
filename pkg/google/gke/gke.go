package gke

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/grafana/cloudcost-exporter/pkg/google/client"
	"github.com/grafana/cloudcost-exporter/pkg/utils"

	"github.com/prometheus/client_golang/prometheus"

	cloudcostexporter "github.com/grafana/cloudcost-exporter"
	"github.com/grafana/cloudcost-exporter/pkg/provider"
)

const subsystem = "gcp_gke"

var (
	gkeNodeMemoryHourlyCostDesc = utils.GenerateDesc(
		cloudcostexporter.MetricPrefix,
		subsystem,
		utils.InstanceMemoryCostSuffix,
		"The memory cost of a GKE Instance in USD/(GiB*h)",
		// Cannot simply use "cluster" because other metric scrapers may add a label for cluster, which would interfere
		[]string{"cluster_name", "instance", "region", "family", "machine_type", "project", "price_tier"},
	)
	gkeNodeCPUHourlyCostDesc = utils.GenerateDesc(
		cloudcostexporter.MetricPrefix,
		subsystem,
		utils.InstanceCPUCostSuffix,
		"The CPU cost of a GKE Instance in USD/(core*h)",
		// Cannot simply use "cluster" because other metric scrapers may add a label for cluster, which would interfere
		[]string{"cluster_name", "instance", "region", "family", "machine_type", "project", "price_tier"},
	)
	persistentVolumeHourlyCostDesc = utils.GenerateDesc(
		cloudcostexporter.MetricPrefix,
		subsystem,
		utils.PersistentVolumeCostSuffix,
		"The cost of a GKE Persistent Volume in USD/h",
		[]string{"cluster_name", "namespace", "persistentvolume", "region", "project", "storage_class", "disk_type", "use_status"},
	)
)

type Config struct {
	Projects       string
	ScrapeInterval time.Duration
}

type Collector struct {
	projects   []string
	regions    []string
	pricingMap *PricingMap
	nodeStore  *NodeStore
	diskStore  *DiskStore
	logger     *slog.Logger
}

func (c *Collector) Register(_ provider.Registry) error {
	return nil
}

func (c *Collector) Collect(ctx context.Context, ch chan<- prometheus.Metric) error {
	now := time.Now()
	nodeCount, diskCount := 0, 0

	select {
	case <-c.nodeStore.Done():
		for _, project := range c.projects {
			for _, instance := range c.nodeStore.GetNodes(project) {
				clusterName := instance.GetClusterName()
				if clusterName == "" {
					c.logger.LogAttrs(ctx, slog.LevelDebug, "instance does not have a cluster name",
						slog.String("region", instance.Region),
						slog.String("machine_type", instance.MachineType),
						slog.String("project", project))
					continue
				}
				cpuCost, ramCost, err := c.pricingMap.GetCostOfInstance(instance)
				if err != nil {
					c.logger.LogAttrs(ctx, slog.LevelError, err.Error(),
						slog.String("machine_type", instance.MachineType),
						slog.String("region", instance.Region),
						slog.String("project", project))
					continue
				}
				labelValues := []string{clusterName, instance.Instance, instance.Region, instance.Family, instance.MachineType, project, instance.PriceTier}
				ch <- prometheus.MustNewConstMetric(gkeNodeCPUHourlyCostDesc, prometheus.GaugeValue, cpuCost, labelValues...)
				ch <- prometheus.MustNewConstMetric(gkeNodeMemoryHourlyCostDesc, prometheus.GaugeValue, ramCost, labelValues...)
				nodeCount++
			}
		}
	default:
		c.logger.LogAttrs(ctx, slog.LevelInfo, "node store not yet populated, skipping node metrics")
	}

	select {
	case <-c.diskStore.Done():
		for _, project := range c.projects {
			for _, d := range c.diskStore.GetDisks(project) {
				prices, err := c.pricingMap.GetCostOfStorage(d.Region(), d.StorageClass())
				if err != nil {
					c.logger.LogAttrs(ctx, slog.LevelError, err.Error(),
						slog.String("disk_name", d.name),
						slog.String("project", project),
						slog.String("region", d.Region()),
						slog.String("cluster_name", d.Cluster),
						slog.String("storage_class", d.StorageClass()))
					continue
				}
				ch <- prometheus.MustNewConstMetric(
					persistentVolumeHourlyCostDesc,
					prometheus.GaugeValue,
					computeDiskCost(d, prices),
					d.Cluster, d.Namespace(), d.Name(), d.Region(), d.Project, d.StorageClass(), d.DiskType(), d.UseStatus(),
				)
				diskCount++
			}
		}
	default:
		c.logger.LogAttrs(ctx, slog.LevelInfo, "disk store not yet populated, skipping disk metrics")
	}

	c.logger.LogAttrs(ctx, slog.LevelInfo, "metrics collected",
		slog.Duration("duration", time.Since(now)),
		slog.Int("nodes_emitted", nodeCount),
		slog.Int("disks_emitted", diskCount))
	return nil
}

func New(ctx context.Context, config *Config, logger *slog.Logger, gcpClient client.Client) (*Collector, error) {
	logger = logger.With("collector", "gke")

	pm, err := NewPricingMap(ctx, gcpClient)
	if err != nil {
		return nil, err
	}

	go func() {
		priceTicker := time.NewTicker(PriceRefreshInterval)
		defer priceTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-priceTicker.C:
				if err := pm.Populate(ctx); err != nil {
					logger.Error(err.Error())
				}
			}
		}
	}()

	projects := strings.Split(config.Projects, ",")
	regions := client.RegionsFromZonesForProjects(gcpClient, projects, logger)

	nodeStore := NewNodeStore(ctx, logger, gcpClient, projects)
	diskStore := NewDiskStore(ctx, logger, gcpClient, projects)

	go func() {
		nodeTicker := time.NewTicker(nodeRefreshInterval)
		defer nodeTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-nodeTicker.C:
				nodeStore.Populate(ctx)
			}
		}
	}()

	go func() {
		diskTicker := time.NewTicker(diskRefreshInterval)
		defer diskTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-diskTicker.C:
				diskStore.Populate(ctx)
			}
		}
	}()

	return &Collector{
		projects:   projects,
		regions:    regions,
		logger:     logger,
		pricingMap: pm,
		nodeStore:  nodeStore,
		diskStore:  diskStore,
	}, nil
}

func (c *Collector) Regions() []string {
	return c.regions
}

func (c *Collector) Name() string {
	return subsystem
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) error {
	ch <- gkeNodeCPUHourlyCostDesc
	ch <- gkeNodeMemoryHourlyCostDesc
	ch <- persistentVolumeHourlyCostDesc
	return nil
}
