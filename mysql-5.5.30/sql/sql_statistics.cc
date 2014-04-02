#include "sql_statistics.h"
#include "sql_plugin.h"
#include "log.h"

STATISTICS_INTERFACE *SIF_server = NULL;

int initialize_statistics_plugin(st_plugin_int *plugin)
{
  SIF_server = (STATISTICS_INTERFACE *)my_malloc(sizeof(STATISTICS_INTERFACE), MYF(MY_WME | MY_ZEROFILL));
  SIF_server->readonly = &opt_readonly;
  if (plugin->plugin->init && plugin->plugin->init(SIF_server))
  {
  	sql_print_error("Plugin '%s' init function returned error.",
  		plugin->name.str);
  	return 1;
  }
  return 0;
}

int finalize_statistics_plugin(st_plugin_int *plugin)
{
  if (plugin->plugin->deinit && plugin->plugin->deinit(SIF_server))
  {
    sql_print_error("Plugin '%s' deinit function returned error.",
        plugin->name.str);
  }
  my_free(SIF_server);
  SIF_server = NULL;
  return 0;
}
