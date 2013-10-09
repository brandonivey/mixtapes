#!/usr/bin/python
import timing
import zipfile
import os
import shlex
from twisted.internet.threads import blockingCallFromThread
from twisted.internet.utils import getProcessValue
import glob
import shutil
import MySQLdb


sql_cred = {
    "host": "tms-db.czsy4cv8uhnr.us-east-1.rds.amazonaws.com",
    "user": "tms_db_server",
    "passwd": "^.E9U6WhiCoY{Fx",
    "db": "tms_db_server"
}


def debug(msg, level=1):
    """
    Outputs messages. More useful that print because it can be silenced.
    """
    #the level doesn't really matter, to be honest
    if level <= 2:
        print(msg)


reactor = None
ROOT_DIR = '/export/brick1'


class Connection:
    '''
    A utility Monad class to handle uploads without having redundant connections
    It supports the with..as syntax (AKA context manager protocol), you can do:

        with Connection() as conn:
            # upload stuff

    That way, it will open and close itself properly, calling __enter__ and
    __exit__ before and after '# upload stuff', respectively
    '''
    url_base = 'http://themixtapesite.com/wp-content/uploads/gravity_forms/1-9e5dc27086c8b2fd2e48678e1f54f98c/2013/02/mixtape2/'
    s3_path = '/export/s3-mixtape2/'

    def __enter__(self):
        """
        Connects to S3 server, establishes connection number
        """
        debug('Setting up connection')
        script_path = os.path.dirname(__file__)
        # Gets the directory in which our script is located, empty if the
        # working directory is the same
        if not script_path:
            script_path = "."
        # We want to get the mixtape counter located in the same directory as
        # the script, not the mixtape counter located in the working directory,
        # as those may not be the same
        self.counter = open(script_path + '/mixtapes.counter', 'r+')
        # Open for writing and reading
        self.count = str(1 + int(self.counter.read()))
        debug("Mixtape counter incremented to %s, making dir" % self.count)
        self.s3_path += self.count
        if not os.path.exists(self.s3_path):
            os.makedirs(self.s3_path)
        return self

    def upload(self, fname, local_dir=".", remote_dir=None):
        """
        Uploads fname from local_dir to remote_dir
        Trailing slash optional
        """
        if remote_dir:
            remote_path = os.path.join(self.s3_path, remote_dir)
            if not os.path.exists(remote_path):
                os.makedirs(remote_path)
        else:
            remote_dir = "."

        debug('Uploading %s from local dir %s to remote dir "%s"' % (
            fname,
            local_dir,
            remote_dir
        ))

        shutil.copy(
            src=os.path.join(local_dir, fname),
            dst=os.path.join(self.s3_path, remote_dir, fname)
        )

    def __exit__(self, type, value, traceback):
        """
        Notifies of errors, updates counter
        """
        if type or value or traceback:
            debug("There has been an error!")
        debug('Closing connection')
        self.url = self.url_base + self.count + '/'
        # Where to find what we've been uploading
        self.counter.seek(0)
        self.counter.write(self.count)
        self.counter.close()


def generate_strip(full_path, target_path):
    '''
    Takes the file located at full_path, removes 1D3 tags and rencodes at
    128kbps, then write that file to target_path
    '''
    debug('Stripping "%s" to "%s"' % (full_path, target_path))

    cmd_string = '/root/bin/ffmpeg -v debug -i "%s" -b:a 128k -loglevel error -map_metadata -1 -map 0:a "%s"' % (
        full_path,
        target_path
    )
    debug('CMD: ' + cmd_string)
    cmd = shlex.split(cmd_string)
    # The command must be an array properly split, shlex does that for us
    # cmd is the first item, *args is the rest of the items

    return_code = blockingCallFromThread(reactor, getProcessValue, cmd[0], cmd[1:])
    # this is a really complicated bit
    # previously, this was done using the normal subprocess module, and hung,
    # since subprocesses are not thread-safe
    # so now, I'm asking the main twisted reactor to call getProcessValue (which
    # is like subprocess.call except it immediatly returns a Deffered and block
    # this thread until the process exits (until the Deffered fires)
    # getProcessValue expects the executable as it's first arg and an array of
    # args as the second

    if return_code != 0:
        debug("Warning: FFMpeg returned nonzero code: %d" % return_code)
        return False
    return True


def generate_preview(full_path, target_path):
    """
    Generates 30 second preview mp3
    """
    debug('Creating preview "%s" to "%s"' % (full_path, target_path))

    cmd_string = '/root/bin/ffmpeg -t 30 -acodec copy -i "%s" "%s"' % (
        full_path,
        target_path
    )
    debug('Executing: ' + cmd_string)
    cmd = shlex.split(cmd_string)

    return_code = blockingCallFromThread(reactor, getProcessValue, cmd[0], cmd[1:])

    if return_code != 0:
        debug("Warning: FFMpeg returned nonzero code: %d" % return_code)
        return False
    return True


def generate_video(full_path, target_path, image_path=None):
    """
    Generates video to be uploaded to youtube
    """
    debug('Creating video from "%s" to "%s"' % (full_path, target_path))

    if image_path:
        cmd_string = 'ffmpeg -loop 1 -i "%s" -i "%s" -c:v libx264 -c:a aac -strict experimental -b:a 192k -shortest "%s"' % (
            image_path,
            full_path,
            target_path
        )
    else:
        cmd_string = 'ffmpeg -loop 1 -i "%s" -c:v libx264 -c:a aac -strict experimental -b:a 192k -shortest "%s"' % (
            full_path,
            target_path
        )
    debug('Executing: ' + cmd_string)
    cmd = shlex.split(cmd_string)

    return_code = blockingCallFromThread(reactor, getProcessValue, cmd[0], cmd[1:])

    if return_code != 0:
        debug("Warning: FFMpeg returned nonzero code: %d" % return_code)
        return False
    return True


def get_images(directory):
    images = []
    images = glob.glob(os.path.join(directory, '*.jpg'))
    return images


def zip_folder(folder, name=None):
    """
    Zips a folder. ZIP will named name.zip if name is given, folder.zip otherwise
    """
    if name is None:
        name = folder
    if not name.endswith(".zip"):
        name += '.zip'
    debug('Zipping "%s" to "%s"' % (folder, name))
    zipped = zipfile.ZipFile(name, 'w')
    # Loops through each file name that folder/* expands too, e.g. every file
    for fname in glob.glob(os.path.join(folder, '*')):
        zipped.write(fname, os.path.basename(fname), zipfile.ZIP_DEFLATED)
        # Add the file located at fname to the archive as the base part of the
        # name, deflating it
    zipped.close()
    # Saves it
    return name


def clear_dir(path="data"):
    """
    Removes every file that doesn't end with .ZIP at path
    """
    for fname in os.listdir(path):
        if not fname.lower().endswith(".zip") and not fname.startswith("."):
            # exclude yet unprocessed ZIPs and hidden files
            os.remove(os.path.join(path, fname))


def process_zip(zip_path, keep_dirs=True, keep_orig=False, save_rest=True):
    """
    Upload, Rencode, Reupload each MP3 in zip_path
    Upload ZIP of all rencoded files
    If keep_dirs is true, temporary files for unzip are not deleted
    If remove_orig is true, the original ZIP will be deleted
    """
    debug("ZIP path: %s\n\
           Keep temporary files: %s\n\
           Keep original ZIP: %s\n\
           Save non-ZIP files: %s" % (zip_path, keep_dirs, keep_orig, save_rest))
    debug("Loading ZIP file for reading")
    mixtape = zipfile.ZipFile(zip_path, 'r')

    BASE_PATH = os.path.dirname(os.path.abspath(__file__))
    FULL_DIR = os.path.join(BASE_PATH, 'full')
    STRIP_DIR = os.path.join(BASE_PATH, 'stripped')
    PREVIEW_DIR = os.path.join(BASE_PATH, 'preview')
    VIDEO_DIR = os.path.join(BASE_PATH, 'video')
    debug('Making temp folders')
    WORKING_DIRS = [FULL_DIR, STRIP_DIR, PREVIEW_DIR, VIDEO_DIR]
    for wdir in WORKING_DIRS:
        if not os.path.exists(wdir):
            os.mkdir(wdir)
    try:
        # Extract each file in the ZIP that ends with mp3 to the full folder
        # and then the stripped folder. If an error is raised, the folders we
        # just just created will be removed ina the finally block of this try.
        for name in mixtape.namelist():
            if name.lower().endswith('mp3') and "MACOSX" not in name:
                basename = os.path.basename(name)
                if not basename.startswith("."):
                    path = os.path.join(FULL_DIR, os.path.basename(name))
                    # Intelligently joins paths, making this script cross-platform
                    debug('Extracting "%s" to "%s"' % (name, path))
                    data = mixtape.read(name)
                    f = open(path, 'w')
                    f.write(data)
                    f.close()
            else:
                # There is a bad file, raising a warning might be in order
                debug('%s does not end on mp3 or is in MACOSX' % name)
        timing.log("Finished extracting", timing.clock() - timing.start)
        # Upload all of the files, stripping copies into the stripped folder
        with Connection() as conn:
            for name in os.listdir(FULL_DIR):
                local_start_time = timing.clock()
                debug('Processing "%s"' % name)
                full_path = os.path.join(FULL_DIR, name)
                stripped_path = os.path.join(STRIP_DIR, name)
                preview_path = os.path.join(PREVIEW_DIR, name)
                if generate_strip(full_path, target_path=stripped_path):
                    conn.upload(name, local_dir=FULL_DIR)
                    conn.upload(name, local_dir=STRIP_DIR, remote_dir="128/")
                else:
                    debug("Not uploading because stripping apaprently failed")
                if generate_preview(full_path, target_path=preview_path):
                    # conn.upload(name, local_dir=PREVIEW_DIR, remote_dir="preview/")
                    video_path = os.path.join(VIDEO_DIR, name)
                    video_path = video_path.replace('mp3', 'mp4')
                    if generate_video(preview_path, video_path):
                        ## upload to youtube
                        # upload_video(video_path)
                        pass
                    else:
                        debug("Unable to generate video file")
                else:
                    debug("Unable to generate preview file")
                timing.log(
                    "Finished processing \"%s\"" % name, timing.clock() - local_start_time
                )
            zipped_name = zip_folder(FULL_DIR, name=os.path.basename(zip_path))
            conn.upload(zipped_name)
            os.remove(zipped_name)
    finally:
        debug('Cleaning up')
        if not keep_dirs:
            for wdir in WORKING_DIRS:
                shutil.rmtree(wdir)
        if not keep_orig:
            os.remove(zip_path)
        if not save_rest:
            clear_dir(os.path.join(ROOT_DIR, "data"))
    url = conn.url + zipped_name
    debug("ZIP processed")
    return url


def get_mixtape_info(post_id):
    """
    Makes an SQL query to get the path to the ZIP assosiated with post_id
    """
    debug("Getting path")
    db = MySQLdb.connect(**sql_cred)
    cur = db.cursor()
    cur.execute('SELECT meta_value FROM tm1_postmeta WHERE post_id = %s AND meta_key = "file_url"' % post_id)
    url = cur.fetchall()[0][0] # First row, first cell returned
    debug("URL: %s" % url)
    path = "data/" + os.path.basename(url)
    cur.close()
    cur = db.cursor()
    cur.execute("SELECT post_title FROM tm1_posts WHERE ID = %s" % post_id)
    post_slug = cur.fetchall()[0][0] # First row, first cell returned
    cur.close()
    db.commit()
    return path, post_slug


def publish_post(post_id, url, post_name):
    """
    Mark post_id as published and processed, set ZIP URL
    """
    debug("Trying to publish post")
    count = 0
    while 1:
        debug("Try #%s" % count)
        try:
            debug("Connecting to MySQL databse")
            db = MySQLdb.connect(**sql_cred)
            cur = db.cursor()
            debug("Setting publish status")
            cur.execute(r'UPDATE tm1_posts SET post_status="publish", post_name="testpostname" WHERE ID = %s;' % post_id)
            debug("Setting ZIP URL")
            cur.execute(r'UPDATE tm1_postmeta SET meta_value="%s" WHERE post_id = %s AND meta_key = "file_url";' % (url, post_id))
            debug("Setting zipping_status to processed")
            cur.execute(r'UPDATE tm1_postmeta SET meta_value="processed" WHERE post_id = %s AND meta_key = "zipping_status";' % post_id)
            #debug('Reset post_name')
            #cur.execute(r'UPDATE tm1_posts SET post_name="test-direct" WHERE ID = %s;' % post_id)
            cur.close()
            db.commit()
            # Save our changes to the database
            debug("Post published")
            break
        except MySQLdb.Error as e:
            debug("MySQL error: %s; Trying again." % e.message)
            count += 1


def process_mixtape(ID):
    """
    Process a mixtape identified by its post's ID
    """
    zip_path, post_slug = get_mixtape_info(ID)
    debug("Path for ZIP: %s" % zip_path)
    debug("Mixtape slug: %s" % post_slug)
    url = process_zip(zip_path, **args)
    # The variable args is searched at the global scope
    # publish_post(int(ID), url, post_slug)
    debug("Mixtape processed")


if __name__ == '__main__':
    # This block will get run only if this module is executed and not imported
    import argparse
    parser = argparse.ArgumentParser(description='Processes an approved\
        mixtape ZIP file and uploads it to S3')
    parser.add_argument('zip_path', help='Path to the ZIP file to be processed')
    parser.add_argument('-k', '--keep-dirs', action="store_true", default=False,
        help="Keep the temporary directories instead of deleteing")
    parser.add_argument('-r', '--keep-orig', action="store_true",
        default=False, help='Remove original ZIP')
    parser.add_argument('-s', '--save-rest', action="store_true", default=True)

    # Process command line arguments
    args = vars(parser.parse_args())
    process_zip(**args)
