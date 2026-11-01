/* TEST_ONLY public stock libudev API; no raw-recv shortcut for the consumer. */
#include <libudev.h>
static struct udev_monitor *stock_monitor(struct udev *udev,const char *channel) {
    struct udev_monitor *monitor=udev_monitor_new_from_netlink(udev,channel);
    if(!monitor||udev_monitor_filter_add_match_subsystem_devtype(monitor,"virtio-ports",NULL)<0||udev_monitor_enable_receiving(monitor)<0)return NULL;
    return monitor;
}
static int stock_consume(struct udev_monitor *monitor,uint64_t *seq) {
    struct udev_device *dev=udev_monitor_receive_device(monitor);
    if(!dev)return 0;
    const char *name=udev_device_get_property_value(dev,"NEON_FIXED_TEST");
    int valid=name&&!strcmp(name,"executed")&&!strcmp(udev_device_get_subsystem(dev),"virtio-ports");
    *seq=udev_device_get_seqnum(dev);udev_device_unref(dev);return valid?1:-1;
}
