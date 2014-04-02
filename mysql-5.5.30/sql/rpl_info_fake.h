#ifndef RPL_INFO_FAKE_H
#define RPL_INFO_FAKE_H

#include "rpl_info_handler.h"

/*
  Please every time you add a new field to the relay log info, update
  what follows. For now, this is just used to get the number of
  fields.
*/
class Rpl_info_fake : public Rpl_info_handler
{
public:
  Rpl_info_fake();
  virtual ~Rpl_info_fake();

private:

  int do_init_info(){return 0;}
  int do_check_info(){return 0;}
  void do_end_info(){}
  int do_flush_info(const bool force){return 0;}
  int do_remove_info(){return 0;}

  int do_prepare_info_for_read(){return 0;}
  int do_prepare_info_for_write(){return 0;}
  bool do_set_info(const int pos, const char *value){return TRUE;}
  bool do_set_info(const int pos, const int value){return TRUE;}
  bool do_set_info(const int pos, const ulong value){return TRUE;}
  bool do_set_info(const int pos, const float value){return TRUE;}
  bool do_set_info(const int pos, const Server_ids *value){return TRUE;}
  bool do_get_info(const int pos, char *value, const size_t size,
                   const char *default_value){return TRUE;}
  bool do_get_info(const int pos, int *value,
                   const int default_value){return TRUE;}
  bool do_get_info(const int pos, ulong *value,
                   const ulong default_value){return TRUE;}
  bool do_get_info(const int pos, float *value,
                   const float default_value){return TRUE;}
  bool do_get_info(const int pos, Server_ids *value,
                   const Server_ids *default_value){return TRUE;}
  char* do_get_description_info(){return NULL;}
  bool do_is_transactional(){return FALSE;}

  Rpl_info_fake& operator=(const Rpl_info_fake& info);
  Rpl_info_fake(const Rpl_info_fake& info);
};

#endif /* RPL_INFO_FAKE_H */
