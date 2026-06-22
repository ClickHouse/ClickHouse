/^<<<<<<< /      { state="ours";   next }
/^\|\|\|\|\|\|\|/ { state="base";   next }
/^=======$/      { state="theirs"; next }
/^>>>>>>> /      { state="";        next }
{ if (state=="" || state=="theirs") print }
