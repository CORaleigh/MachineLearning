How to setup your Windows 10 environment for TensorFlow

This guide by Mark Labinski is very good: https://medium.com/@marklabinski/installing-tensorflow-object-detection-api-on-windows-10-7a4eb83e1e7b

There were a few things I had to do that weren't covered by it.

At step 4, I did have to install Anaconda and setup a python 3.5 environment within Anaconda, by using the Anaconda cmd prompt. Here's some documentation by Anaconda on this: http://docs.anaconda.com/anaconda/user-guide/faq/#how-do-i-get-the-latest-anaconda-with-python-3-5

After installing Anaconda, open the Anaconda prompt and type: conda create --name py35 python=3.5 example: (base) C:\Users\shermanp>conda create --name py35 python=3.5

The (base) is your current anaconda environment. Say yes to proceed to install new packages.

Then type: conda activate py35 example: (base) C:\Users\shermanp>conda activate py35 'chcp' is not recognized as an internal or external command, operable program or batch file. (you can ignore this if you get it)

Upgrade pip:

(py35) C:\Users\shermanp>python -m pip install --upgrade pip

Then pip install tensorflow or tensorflow-gpu:

(py35) C:\Users\shermanp>pip install tensorflow-gpu

Then pillow, lxml, cv2, matplotlib, and scipy:

(py35) C:\Users\shermanp>pip install pillow (py35) C:\Users\shermanp>pip install lxml

I change my directory to the TensorFlow models directory but it's not needed: (py35) C:\Users\shermanp\code\vehicle_counting_tensorflow>pip install opencv-python (py35) C:\Users\shermanp\code\vehicle_counting_tensorflow>pip install matplotlib (py35) C:\Users\shermanp\code\vehicle_counting_tensorflow>pip install scipy

(Mark writes in step 5 that installing the dependencies isn't necessary if using an Anaconda environment, but I did have to).

Then return to Mark's guide for installing the COCO API, compiling protobuf, and adding to your path variable.
