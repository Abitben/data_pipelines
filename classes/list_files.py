import os

class FolderLister:
  def __init__(self, folder_path):
    self.folder_path = folder_path

  def list_folders(self):
    folder_list = []
    for item in os.listdir(self.folder_path):
      item_path = os.path.join(self.folder_path, item)
      if os.path.isdir(item_path):
        folder_list.append(item)
    return folder_list