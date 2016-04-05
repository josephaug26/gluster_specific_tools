#include "list.h"
#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <stdarg.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>


#define MAX_VALUE 0xFF
#define BLOCK_SIZE 1024

typedef unsigned char uchar_t;
typedef signed char schar_t;

typedef enum boolean{
        _false = 0,
        _true
} boolean_t;




/**************************LOGGING*********************************************/
/*logging related types, macros and functions*/
typedef enum log_level {
        log_error = -1,
        log_info = 0
}log_level_t;


/* Function used for logging */
void
log_it (log_level_t     log_level,
        const char       *file_name,
        const char       *function,
        int             line,
        const char      *fmt, ...)
{
        FILE *stream = NULL;
        va_list arg_list;
        char *message = NULL;

        if (log_level == -1)
                stream = stderr;
        else if (log_level == 0)
                stream = stdout;

        /* form the message */
        va_start (arg_list, fmt);
        vasprintf (&message, fmt, arg_list);
        va_end (arg_list);

        fprintf (stream, "%d %s %s : %s\n", line, file_name, function, message);

        free (message);
}


/* Macro used for logging */
#define LOG_IT(log_level, fmt...)\
do {\
        log_it (log_level, __FILE__, __FUNCTION__, __LINE__, ##fmt);\
} while(0)
/******************************************************************************

 * UUID RELATED

*******************************************************************************/
/* 
 * If linux/types.h is already been included, assume it has defined
 * everything we need.  (cross fingers)  Other header files may have 
 * also defined the types that we need.
 */
#if (!defined(_STDINT_H) && !defined(_UUID_STDINT_H))
#define _UUID_STDINT_H

typedef unsigned char uint8_t;
typedef signed char int8_t;

#if (4 == 8)
typedef int		int64_t;
typedef unsigned int	uint64_t;
#elif (8 == 8)
typedef long		int64_t;
typedef unsigned long	uint64_t;
#elif (8 == 8)
#if defined(__GNUC__)
typedef __signed__ long long 	int64_t;
#else
typedef signed long long 	int64_t;
#endif
typedef unsigned long long	uint64_t;
#endif

#if (4 == 2)
typedef	int		int16_t;
typedef	unsigned int	uint16_t;
#elif (2 == 2)
typedef	short		int16_t;
typedef	unsigned short	uint16_t;
#else
  ?==error: undefined 16 bit type
#endif

#if (4 == 4)
typedef	int		int32_t;
typedef	unsigned int	uint32_t;
#elif (8 == 4)
typedef	long		int32_t;
typedef	unsigned long	uint32_t;
#elif (2 == 4)
typedef	short		int32_t;
typedef	unsigned short	uint32_t;
#else
 ?== error: undefined 32 bit type
#endif

#endif



typedef unsigned char uuid_t[16];

struct uuid {
	uint32_t	time_low;
	uint16_t	time_mid;
	uint16_t	time_hi_and_version;
	uint16_t	clock_seq;
	uint8_t	node[6];
};

void gf_uuid_copy(uuid_t dst, const uuid_t src)
{
	unsigned char		*cp1;
	const unsigned char	*cp2;
	int			i;

	for (i=0, cp1 = dst, cp2 = src; i < 16; i++)
		*cp1++ = *cp2++;
}




void uuid_unpack(const uuid_t in, struct uuid *uu)
{
	const uint8_t	*ptr = in;
	uint32_t		tmp;

	tmp = *ptr++;
	tmp = (tmp << 8) | *ptr++;
	tmp = (tmp << 8) | *ptr++;
	tmp = (tmp << 8) | *ptr++;
	uu->time_low = tmp;

	tmp = *ptr++;
	tmp = (tmp << 8) | *ptr++;
	uu->time_mid = tmp;

	tmp = *ptr++;
	tmp = (tmp << 8) | *ptr++;
	uu->time_hi_and_version = tmp;

	tmp = *ptr++;
	tmp = (tmp << 8) | *ptr++;
	uu->clock_seq = tmp;

	memcpy(uu->node, ptr, 6);
}

static const char *fmt_lower =
	"%08x-%04x-%04x-%02x%02x-%02x%02x%02x%02x%02x%02x";

static const char *fmt_upper =
	"%08X-%04X-%04X-%02X%02X-%02X%02X%02X%02X%02X%02X";

#ifdef UUID_UNPARSE_DEFAULT_UPPER
#define FMT_DEFAULT fmt_upper
#else
#define FMT_DEFAULT fmt_lower
#endif

static void gf_uuid_unparse_x(const uuid_t uu, char *out, const char *fmt)
{
	struct uuid uuid;

	uuid_unpack(uu, &uuid);
	sprintf(out, fmt,
		uuid.time_low, uuid.time_mid, uuid.time_hi_and_version,
		uuid.clock_seq >> 8, uuid.clock_seq & 0xFF,
		uuid.node[0], uuid.node[1], uuid.node[2],
		uuid.node[3], uuid.node[4], uuid.node[5]);
}

void gf_uuid_unparse_lower(const uuid_t uu, char *out)
{
	gf_uuid_unparse_x(uu, out,	fmt_lower);
}

void gf_uuid_unparse_upper(const uuid_t uu, char *out)
{
	gf_uuid_unparse_x(uu, out,	fmt_upper);
}

void gf_uuid_unparse(const uuid_t uu, char *out)
{
	gf_uuid_unparse_x(uu, out, FMT_DEFAULT);
}


/******************************************************************************/




#define GF_VALIDATE_OR_GOTO(name,arg,label)   do {                      \
		if (!arg) {                                             \
			errno = EINVAL;                                 \
			LOG_IT (log_error, "invalid argument: " #arg);	\
			goto label;                                     \
		}                                                       \
	} while (0)

#define GFDB_DATA_STORE               "gfdbdatastore"

#ifdef NAME_MAX
#define GF_NAME_MAX NAME_MAX
#else
#define GF_NAME_MAX 255
#endif

/*Structure to hold the link information*/
typedef struct gfdb_link_info {
        uuid_t                          pargfid;
        char                            file_name[GF_NAME_MAX];
        struct list_head                list;
} gfdb_link_info_t;


/*Structure used for querying purpose*/
typedef struct gfdb_query_record {
        uuid_t                          gfid;
        /*This is the hardlink list*/
        struct list_head                link_list;
        int                             link_count;
} gfdb_query_record_t;


/*Create a single link info structure*/
gfdb_link_info_t*
gfdb_link_info_new ()
{
        gfdb_link_info_t *link_info = NULL;

        link_info = calloc (1, sizeof(gfdb_link_info_t));
        if (!link_info) {
                LOG_IT (log_error, "Memory allocation failed for "
                        "link_info ");
                goto out;
        }

        INIT_LIST_HEAD (&link_info->list);

out:

        return link_info;
}

/*Destroy a link info structure*/
void
gfdb_link_info_free(gfdb_link_info_t *link_info)
{
	if (link_info)
        	free (link_info);
}


/*Function to create the query_record*/
gfdb_query_record_t *
gfdb_query_record_new()
{
        int ret = -1;
        gfdb_query_record_t *query_record = NULL;

        query_record = calloc (1, sizeof(gfdb_query_record_t));
        if (!query_record) {
                LOG_IT (log_error, "Memory allocation failed for "
                        "query_record ");
                goto out;
        }

        INIT_LIST_HEAD (&query_record->link_list);

        ret = 0;
out:
        if (ret == -1) {
                if (query_record) { (query_record); }
        }
        return query_record;
}


/*Function to delete a single linkinfo from list*/
static void
gfdb_delete_linkinfo_from_list (gfdb_link_info_t **link_info)
{
        GF_VALIDATE_OR_GOTO (GFDB_DATA_STORE, link_info, out);
        GF_VALIDATE_OR_GOTO (GFDB_DATA_STORE, *link_info, out);

        /*Remove hard link from list*/
        list_del(&(*link_info)->list);
        gfdb_link_info_free (*link_info);
        link_info = NULL;
out:
        return;
}


/*Function to destroy link_info list*/
void
gfdb_free_link_info_list (gfdb_query_record_t *query_record)
{
        gfdb_link_info_t        *link_info = NULL;
        gfdb_link_info_t        *temp = NULL;

        GF_VALIDATE_OR_GOTO (GFDB_DATA_STORE, query_record, out);

        list_for_each_entry_safe(link_info, temp,
                        &query_record->link_list, list)
        {
                gfdb_delete_linkinfo_from_list (&link_info);
                link_info = NULL;
        }

out:
        return;
}



/* Function to add linkinfo to the query record */
int
gfdb_add_link_to_query_record (gfdb_query_record_t      *query_record,
                           uuid_t                   pgfid,
                           char               *base_name)
{
        int ret                                 = -1;
        gfdb_link_info_t *link_info             = NULL;
        int base_name_len                       = 0;

        GF_VALIDATE_OR_GOTO (GFDB_DATA_STORE, query_record, out);
        GF_VALIDATE_OR_GOTO (GFDB_DATA_STORE, pgfid, out);
        GF_VALIDATE_OR_GOTO (GFDB_DATA_STORE, base_name, out);

        link_info = gfdb_link_info_new ();
        if (!link_info) {
                goto out;
        }

        gf_uuid_copy (link_info->pargfid, pgfid);
        base_name_len = strlen (base_name);
        memcpy (link_info->file_name, base_name, base_name_len);
        link_info->file_name[base_name_len] = '\0';

        list_add_tail (&link_info->list,
                        &query_record->link_list);

        query_record->link_count++;

        ret = 0;
out:
        if (ret) {
                gfdb_link_info_free (link_info);
                link_info = NULL;
        }
        return ret;
}



/*Function to destroy query record*/
void
gfdb_query_record_free(gfdb_query_record_t *query_record)
{
        if (query_record) {
                gfdb_free_link_info_list (query_record);
                if (query_record) { (query_record); }
        }
}


/******************************************************************************
                SERIALIZATION/DE-SERIALIZATION OF QUERY RECORD
*******************************************************************************/
/******************************************************************************
 The on disk format of query record is as follows,

+---------------------------------------------------------------------------+
| Length of serialized query record |       Serialized Query Record         |
+---------------------------------------------------------------------------+
             4 bytes                     Length of serialized query record
                                                      |
                                                      |
     -------------------------------------------------|
     |
     |
     V
   Serialized Query Record Format:
   +---------------------------------------------------------------------------+
   | GFID |  Link count   |  <LINK INFO>  |.....                      | FOOTER |
   +---------------------------------------------------------------------------+
     16 B        4 B         Link Length                                  4 B
                                |                                          |
                                |                                          |
   -----------------------------|                                          |
   |                                                                       |
   |                                                                       |
   V                                                                       |
   Each <Link Info> will be serialized as                                  |
   +-----------------------------------------------+                       |
   | PGID | BASE_NAME_LENGTH |      BASE_NAME      |                       |
   +-----------------------------------------------+                       |
     16 B       4 B             BASE_NAME_LENGTH                           |
                                                                           |
                                                                           |
   ------------------------------------------------------------------------|
   |
   |
   V
   FOOTER is a magic number 0xBAADF00D indicating the end of the record.
   This also serves as a serialized schema validator.
 * ****************************************************************************/

#define GFDB_QUERY_RECORD_FOOTER 0xBAADF00D
#define UUID_LEN                 16

static boolean_t
is_serialized_buffer_valid (char *in_buffer, int buffer_length) {
        boolean_t       ret        = _false;
        int             footer     = 0;

        /* Read the footer */
        in_buffer += (buffer_length - sizeof (int32_t));
        memcpy (&footer, in_buffer, sizeof (int32_t));

        /*
         * if the footer is not GFDB_QUERY_RECORD_FOOTER
         * then the serialized record is invalid
         *
         * */
        if (footer != GFDB_QUERY_RECORD_FOOTER) {
                goto out;
        }

        ret = _true;
out:
        return ret;
}


static int
gfdb_query_record_deserialize (char *in_buffer,
                               int buffer_length,
                               gfdb_query_record_t **query_record)
{
        int ret                                 = -1;
        char *buffer                            = NULL;
        int i                                   = 0;
        gfdb_link_info_t *link_info             = NULL;
        int count                               = 0;
        int base_name_len                       = 0;
        gfdb_query_record_t *ret_qrecord        = NULL;

        GF_VALIDATE_OR_GOTO (GFDB_DATA_STORE, in_buffer, out);
        GF_VALIDATE_OR_GOTO (GFDB_DATA_STORE, query_record, out);
        GF_VALIDATE_OR_GOTO (GFDB_DATA_STORE, (buffer_length > 0), out);

        if (!is_serialized_buffer_valid (in_buffer, buffer_length)) {
                LOG_IT (log_error, "Invalid serialized query record");
                goto out;
        }

        buffer = in_buffer;

        ret_qrecord = gfdb_query_record_new ();
        if (!ret_qrecord) {
                LOG_IT (log_error, "Failed to allocate space to "
                        "gfdb_query_record_t");
                goto out;
        }

        /* READ GFID */
        memcpy ((ret_qrecord)->gfid, buffer, UUID_LEN);
        buffer += UUID_LEN;
        count += UUID_LEN;

        /* Read the number of link */
        memcpy (&(ret_qrecord->link_count), buffer, sizeof (int32_t));
        buffer += sizeof (int32_t);
        count += sizeof (int32_t);

        /* Read all the links */
        for (i = 0; i < ret_qrecord->link_count; i++) {
                if (count >= buffer_length) {
                        LOG_IT (log_error, "Invalid serialized "
                                "query record");
                        ret = -1;
                        goto out;
                }

                link_info = gfdb_link_info_new ();
                if (!link_info) {
                        LOG_IT (log_error, "Failed to create link_info");
                        goto out;
                }

                /* READ PGFID */
                memcpy (link_info->pargfid, buffer, UUID_LEN);
                buffer += UUID_LEN;
                count += UUID_LEN;

                /* Read base name length */
                memcpy (&base_name_len, buffer, sizeof (int32_t));
                buffer += sizeof (int32_t);
                count += sizeof (int32_t);

                /* READ basename */
                memcpy (link_info->file_name, buffer, base_name_len);
                buffer += base_name_len;
                count += base_name_len;
                link_info->file_name[base_name_len] = '\0';

                /* Add link_info to the list */
                list_add_tail (&link_info->list,
                               &(ret_qrecord->link_list));

                /* Reseting link_info */
                link_info = NULL;
        }

        ret = 0;
out:
        if (ret) {
                gfdb_query_record_free (ret_qrecord);
                ret_qrecord = NULL;
        }
        *query_record = ret_qrecord;
        return ret;
}



/* Function to read query record from file.
 * Allocates memory to query record and
 * returns length of serialized query record when successful
 * Return -1 when failed.
 * Return 0 when reached EOF.
 * */
int
gfdb_read_query_record (int fd,
                        gfdb_query_record_t **query_record)
{
        int ret                 = -1;
        int buffer_len          = 0;
        int read_len            = 0;
        char *buffer            = NULL;
        char *read_buffer       = NULL;

        GF_VALIDATE_OR_GOTO (GFDB_DATA_STORE, (fd >= 0), out);
        GF_VALIDATE_OR_GOTO (GFDB_DATA_STORE, query_record, out);


        /* Read serialized query record length from the file*/
        ret = read (fd, &buffer_len, sizeof (int32_t));
        if (ret < 0) {
                LOG_IT (log_error, "Failed reading buffer length"
                                " from file");
                goto out;
        }
        /* EOF */
        else if (ret == 0) {
                ret = 0;
                goto out;
        }

        /* Allocating memory to the serialization buffer */
        buffer = calloc (1, buffer_len);
        if (!buffer) {
                LOG_IT (log_error, "Failed to allocate space to "
                        "serialized buffer");
                goto out;
        }


        /* Read the serialized query record from file */
        read_len = buffer_len;
        read_buffer = buffer;
        while ((ret = read (fd, read_buffer, read_len)) < read_len) {

                /*Any error */
                if (ret < 0) {
                        LOG_IT (log_error, "Failed to read serialized "
                                "query record from file");
                        goto out;
                }
                /* EOF */
                else if (ret == 0) {
                        LOG_IT (log_error, "Invalid query record or "
                                "corrupted query file");
                        ret = -1;
                        goto out;
                }

                read_buffer += ret;
                read_len -= ret;
        }

        ret = gfdb_query_record_deserialize (buffer, buffer_len,
                                             query_record);
        if (ret) {
                LOG_IT (log_error, "Failed to de-serialize query record");
                goto out;
        }

        ret = buffer_len;
out:
        if (buffer) { free(buffer);}
        return ret;
}


/******************************************************************************
 * 
 *                      Main ()
 * 
 * ****************************************************************************/

#define STR_TAB "        "

void
usage(){
        LOG_IT (log_error, "Usage : gfdb_query_file_reader <query_file_path>");
}


int
main ( int argc, char *argv[] ) {

        int ret                                 = -1;
        struct stat stat_buff                   = {0};
        char *query_file_path                   = NULL;
        int query_fd                            = -1;
        gfdb_query_record_t *query_record       = NULL;
        char uuid_str[100]                      ="";
        gfdb_link_info_t *link_info             = NULL;

        if (argc != 2) {
                usage();
                goto out;
        }

	query_file_path = argv[1];

        ret = stat (query_file_path, &stat_buff);
        if (ret) {
                LOG_IT (log_error, "%s query file doesnt exist : %s",
                          query_file_path, strerror (errno));
                goto out;
        }

        query_fd = open (query_file_path, O_RDONLY);
        if (query_fd < 0) {
                LOG_IT (log_error, "Failed to open %s", query_file_path);
                goto out;
        }

        while((ret = gfdb_read_query_record
                        (query_fd, &query_record)) != 0) {

                if (ret < 0 && !query_record) {
                         LOG_IT (log_error,"Failed to fetch query record "
                                "from query file");
                        goto out;
                }

                gf_uuid_unparse (query_record->gfid, uuid_str);
                printf("GFID : %s\n", uuid_str);

                list_for_each_entry (link_info, &query_record->link_list,
                                    list) {
                        
                        gf_uuid_unparse (link_info->pargfid, uuid_str);
                        printf("%sPGFID : %s, BASE_NAME: %s \n", STR_TAB,
                                uuid_str, link_info->file_name);
                }

                free (query_record);
                query_record = NULL;
                
        }

        ret = 0;
out:

        if (query_fd!=-1) {
                close (query_fd);
        }

        return ret;
}
